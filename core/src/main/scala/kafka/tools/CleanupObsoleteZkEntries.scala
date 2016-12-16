/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import joptsimple.OptionParser
import kafka.utils._
import kafka.utils.ZkUtils._
import org.apache.zookeeper.data.Stat
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import scala.util.parsing.json._

/**
 * This tool is removes obsolete znodes from Zookeeper server. Many use cases involve transient consumers 
 * which end up creating bunch of entries (group and offsets) in Zookeeper. This tool can perform cleanup 
 * based on topic name or group name.
 * 
 * If topic name is provided as input, it would scan through consumer group entries and delete any that 
 * had no offset update since the given date.
 * 
 * If group name is provided as input, it would scan through all the topics in the group and perform 
 * deletion if there were no updates since the time provided. 
 */

object CleanupObsoleteZkEntries extends Logging { 

  /* These are some default values used for zookeeper connection */
  private val ZkSessionTimeout: Int = 10000
  private val ZkConnectionTimeout: Int = 10000
  
  /* The zookeeper connection string (host:port) */
  private var zkConnect: String = null
  
  /* The mode in which the tool would run */
  private var deleteBy: String = null
  
  /* The name of the topic or group to be cleaned up */
  private var topicOrGroupName: String = null
  
  /* Time threshold. Znodes with modified time < 'since' are deleted */
  private var since: Long = -1
  
  /* A flag which indicates if the tool runs in passive mode w/o actually deleting anything */
  private var dryRun: Boolean = false
  
  def main(args: Array[String]) {
    var zkClient: ZkClient = null
    
    try {
      parseArguments(args)    
      info("Connecting to zookeeper instance at " + zkConnect)
      zkClient = new ZkClient(zkConnect, ZkSessionTimeout, ZkConnectionTimeout, ZKStringSerializer)
      	  
      if(deleteBy == "group") {
		    info("Deleting by group")
        removeObsoleteConsumerGroups(zkClient, topicOrGroupName, since)
	  }
      else	{
		    info("Deleting by topic")
        removeObsoleteConsumerTopics(zkClient, topicOrGroupName, since)
	  }

      info("Kafka obsolete Zk entries cleanup tool shutdown successfully.")
    } catch {
      case e: Throwable =>
        warn("removal failed because of " + e.getMessage)
        warn(Utils.stackTrace(e))
    } finally {
      if (zkClient != null)
        zkClient.close()
    }
  }
  
  /**
   * Performs 2 initial checks and then based on the value of time threshold provided, 
   * would delete the obsolete topics in the group 
   * 
   * @param zkClient Zookeeper client object which is already connected
   * @param group The group-ID
   * @param since The time threshold considered for deletion
   */
  def removeObsoleteConsumerGroups(zkClient: ZkClient, group: String, since: Long)
  {
    val dirs: ZKGroupDirs = new ZKGroupDirs(group)
    
    if(!pathExists(zkClient, dirs.consumerGroupDir)) {
      warn("Path " + dirs.consumerGroupDir + " doesn't exist on the zookeeper instance. Aborted.")
      return
    }

    if(checkIfLiveGroupConsumers(zkClient, group)) {
      warn("Aborted.")
      return
    }
    
    if(since == -1) {        
      // If no threshold for time provided, delete entire group
      deleteZNodeRecursive(zkClient, dirs.consumerGroupDir)
    }
    else {
      val childTopics = getChildren(zkClient, dirs.consumerGroupDir + "/offsets")
      var numChildrenTopics:Int = childTopics.length

      for(topic <- childTopics) {
        val topicPaths = new ZKGroupTopicDirs(group, topic)
        numChildrenTopics = removeBrokerPartitionPairs(zkClient, topicPaths, since, numChildrenTopics)
      }
      deleteGroupIfNoTopics(zkClient, dirs, numChildrenTopics)
    }
  }
  
  /**
   * Scans all the groups to find those which have the required topic. If the topic is present and 
   * there are no active consumers in that group, it would perform the deletion considering the time
   * threshold 
   * 
   * @param zkClient Zookeeper client object which is already connected
   * @param topic The topic
   * @param since The time threshold considered for deletion
   */
  def removeObsoleteConsumerTopics(zkClient: ZkClient, topic: String, since: Long) 
  {
    val stat = new Stat()
    val childGroups = getChildren(zkClient, ZkUtils.ConsumersPath)
    
    for(group <- childGroups) {
      val topicPaths = new ZKGroupTopicDirs(group, topic)
      if((pathExists(zkClient, topicPaths.consumerOffsetDir) || pathExists(zkClient, topicPaths.consumerOwnerDir) || checkIdNode(zkClient, group, topic))
        && !checkIfLiveTopicConsumers(zkClient, group, topic)) {
        if(since == -1) {     // If no time threshold provided, delete entire topic
          deleteZNodeRecursive(zkClient, topicPaths.consumerGroupDir)          
        }
        else {
          zkClient.readData(topicPaths.consumerGroupDir + "/offsets", stat)
          var numChildrenTopics: Int = stat.getNumChildren
          numChildrenTopics = removeBrokerPartitionPairs(zkClient, topicPaths, since, numChildrenTopics)
          deleteGroupIfNoTopics(zkClient, topicPaths, numChildrenTopics)
        }        
      }
    }
  }

  /**
   * Scans the "broker-partition" entries inside a topic and performs deletion if there were no
   * updates since the threshold date provided. Also, deletes the topic itself if there are no more 
   * child entries under it. 
   * 
   * @param zkClient Zookeeper client object which is already connected
   * @param dirs A convenient ZKGroupTopicDirs object for quick access to paths 
   * @param since The time threshold considered for deletion
   */
  def removeBrokerPartitionPairs(zkClient: ZkClient, dirs: ZKGroupTopicDirs, since: Long,
                           numChildrenTopics: Int) : Int =  {
    val stat = new Stat()
    val childBrokerPartitionPair = getChildren(zkClient, dirs.consumerOffsetDir)
    var numChildrenPairs:Int = childBrokerPartitionPair.length
    
    for(brokerPartitionPair <- childBrokerPartitionPair) {
      val brokerPartitionPath:String = dirs.consumerOffsetDir + "/" + brokerPartitionPair
      zkClient.readData(brokerPartitionPath, stat)
      debug("modified time for " + brokerPartitionPath + " is " + stat.getMtime)
      
      // delete the node if was never modified after 'since', the threshold timestamp
      if(stat.getMtime < since) {
        deleteZNode(zkClient, brokerPartitionPath)
        numChildrenPairs = numChildrenPairs - 1
      }
    } 
    
    // if the topic is empty, then we can delete it
    if(numChildrenPairs == 0) { 
      deleteZNode(zkClient, dirs.consumerOffsetDir)
      return numChildrenTopics - 1
  }
    numChildrenTopics
  }
    
  /**
   * If there are no more topics under this group, delete the entire group
   * 
   * @param zkClient Zookeeper client object which is already connected
   * @param dirs A convenient ZKGroupDirs object for quick access to consumer paths
   * @param numTopics Number of topics present 
   */
  def deleteGroupIfNoTopics(zkClient: ZkClient, dirs: ZKGroupDirs, numTopics: Int) {
    if(numTopics == 0) {
      debug("No topics left in the group \"" + dirs.consumerGroupDir + "\".")
      deleteZNodeRecursive(zkClient, dirs.consumerGroupDir)
    }
  }

  /**
    * Performs a defensive check to ensure that there are no consumers currently registered under the group
    *
    * @param zkClient Zookeeper client object which is already connected
    * @param group The group-ID
    * @return Boolean indicating if there are live consumers or not
    */
  def checkIfLiveGroupConsumers(zkClient: ZkClient, group: String) : Boolean = {
    try{
      val activeConsumers = getConsumersInGroup(zkClient, group)

      if(activeConsumers.nonEmpty) {
        warn("The group \"" + group + "\" has active consumer(s).")
        return true
      }
      debug("No live consumers found for group \"" + group + "\".")
      false
    } catch {
      case e: ZkNoNodeException =>
        warn("assuming no active consumers due to " + e.getMessage)
        false
    }
  }
  
  /**
   * Performs a defensive check to ensure that there are no consumers currently registered under the group 
   * 
   * @param zkClient Zookeeper client object which is already connected
   * @param group The group-ID
   * @param topic The topic name
   * @return Boolean indicating if there are live consumers or not
   */
  def checkIfLiveTopicConsumers(zkClient: ZkClient, group: String, topic: String) : Boolean = {
    try{
      //val activeConsumers = getConsumersInGroup(zkClient, groupID)
      val dirs = new ZKGroupTopicDirs(group, topic)
      val activeConsumers = getChildren(zkClient, dirs.consumerOwnerDir)

      if(activeConsumers.nonEmpty) {
        warn("The group \"" + group + "\" has active consumer(s).")
        return true
      }
      debug("No live consumers found for group \"" + group + "\".")
      false
	  } catch {
      case e: ZkNoNodeException =>
        warn("assuming no active consumers due to " + e.getMessage)
        false
	  }
  }

  /**
    * Performs a check to see if the Node Data for the ids directory includes the topic name
    *
    * @param zkClient Zookeeper client object which is already connected
    * @param group The group-ID
    * @param topic The topic name
    * @return Boolean indicating that the id node either does not exist or contains the topic name in the node data
    */
  def checkIdNode(zkClient: ZkClient, group: String, topic: String) : Boolean = {
    try{
      val dirs = new ZKGroupTopicDirs(group, topic)
      val idNode : Seq[String] = getChildren(zkClient, dirs.consumerRegistryDir)

      if(idNode.isEmpty) {
        debug("The id node for \"" + group + "\" does not exist.")
        return false
      }
      info("Found id node for \"" + group + "\". Checking for topic in node data.")
      val idPath:String = dirs.consumerRegistryDir + "/" + idNode.head
      val nodeData = JSON.parseFull(zkClient.readData[String](idPath)) match {
        case Some(data) => data
        case _ => return false
      }

      val subscriptionData = nodeData.asInstanceOf[Map[String, Any]].get("subscription") match {
        case Some(data) => data
        case _ => return false
      }

      if(subscriptionData.asInstanceOf[Map[String, Int]].contains(topic))  {
        info("Topic \"" + topic + "\" exists in " + idPath)
        return true
      }
      false
    } catch {
      case e: ZkNoNodeException =>
        warn("assuming no id node exists due to " + e.getMessage)
        false
    }
  }

  /**
   * Delete a znode if "dry-run" is off.
   *
   * @param zkClient Zookeeper client object which is already connected
   * @param path The path of znode to be deleted
   */
  def deleteZNode(zkClient: ZkClient, path: String) {
   info("Deleting \"" + path + "\".")
    if(!dryRun)
      deletePath(zkClient, path)  
  }
  
  /**
   * Delete a znode recursively if "dry-run" is off.
   *
   * @param zkClient Zookeeper client object which is already connected
   * @param path The path of znode to be deleted
   */
  def deleteZNodeRecursive(zkClient: ZkClient, path: String) {
    info("Deleting \"" + path + "\" recursively.")
    if(!dryRun)
      deletePathRecursive(zkClient, path)  
  }
  
  /**
   * Parse the i/p arguments and set the member variables
   * 
   * @param args Input arguments to the tool
   */
  def parseArguments(args: Array[String]) {
    
    val parser = new OptionParser
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED parameter. The connection string " +
                                      "(host:port) for zookeeper connection.")
                           .withRequiredArg
                           .describedAs("zookeeper")
                           .ofType(classOf[String])
                           
    val deleteByOpt = parser.accepts("delete-by", "REQUIRED parameter. Valid values are: " +
    		                             "\"topic\" and \"group\" indicate deletion based on " +
    		                             "a specific topic/group respectively.")
                         .withRequiredArg
                         .describedAs("delete-by")
                         .ofType(classOf[String])
                           
    val topicOrGroupOpt = parser.accepts("name", "REQUIRED parameter. Provide the " +
    		                                 "name of the topic or group, based on \"delete-by\", " +
    		                                 "which you want to cleanup.")
                         .withRequiredArg
                         .describedAs("name")
                         .ofType(classOf[String])

    val sinceOpt = parser.accepts("since", "REQUIRED parameter. Time elapsed since the epoch " +
    		                          "considered as threshold. Znodes with modified time before " +
    		                          "this timestamp are deleted. Use \"-1\" to skip the check " +
    		                          "for modified time.")
                           .withRequiredArg
                           .describedAs("since")
                           .ofType(classOf[String])

    val dryRunOpt = parser.accepts("dry-run", "OPTIONAL parameter. Passing \"--dry-run\" will " +
    		                           "cause this tool to run in passive mode w/o actually deleting " +
    		                           "anything from zookeeper but logging all the activities that it " +
    		                           "would perform. It is highly recommended to use this setting if " +
    		                           "you don't want to risk deleting things and just want to see " +
    		                           "znodes which are eligible for deletion.")
                           .withOptionalArg
                           .describedAs("dry-run")
                           .ofType(classOf[String])

    val options = parser.parse(args : _*)

    for(arg <- List(deleteByOpt, topicOrGroupOpt, zkConnectOpt, sinceOpt, topicOrGroupOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }

    deleteBy = options.valueOf(deleteByOpt)
    if (deleteBy != "topic" && deleteBy != "group") {
        System.err.println("Invalid value of \"delete-by\" passed : \"" + deleteBy + "\"")
        System.exit(1)
    }
    
    topicOrGroupName = options.valueOf(topicOrGroupOpt)
    zkConnect = options.valueOf(zkConnectOpt)
    since = options.valueOf(sinceOpt).toLong
 
    if (options.has(dryRunOpt)) {
       dryRun = true
       this.logIdent = "[dry-run] "
    }
  }
}