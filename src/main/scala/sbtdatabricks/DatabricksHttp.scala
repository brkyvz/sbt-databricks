/*
 * Copyright 2015 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sbtdatabricks

import java.io.PrintStream

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import org.eclipse.jetty.client.{HttpResponseException, HttpClient}
import org.eclipse.jetty.client.api.Request
import org.eclipse.jetty.client.util.BasicAuthentication
import org.eclipse.jetty.http.{HttpHeader, HttpFields, HttpField}
import org.eclipse.jetty.util.ssl.SslContextFactory

import sbt._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import sbtdatabricks.DatabricksPlugin.ClusterName
import sbtdatabricks.DatabricksPlugin.autoImport.DBC_ALL_CLUSTERS
import sbtdatabricks.util.requests._

/** Collection of REST calls to Databricks Cloud and related helper functions. Exposed for tests */
class DatabricksHttp(
    endpoint: String,
    val client: HttpClient,
    outputStream: PrintStream = System.out) {

  import DatabricksHttp.mapper

  /**
   * Upload a jar to Databricks Cloud.
   * @param name Name of the library to show on Databricks Cloud
   * @param file The jar file
   * @return The path of the file in dbfs
   */
  private[sbtdatabricks] def uploadJar(
      name: String,
      file: File,
      folder: String): UploadedLibraryId = {
    outputStream.println(s"Uploading $name")
    mapper.readValue[UploadedLibraryId](send(UploadLibraryRequest(name, file, folder)))
  }

  /**
   * Deletes the given Library on Databricks Cloud
   * @param libraryId the id for the library
   * @return The response from Databricks Cloud, i.e. the libraryId
   */
  private[sbtdatabricks] def deleteJar(libraryId: String): String = {
    send(DeleteLibraryRequestV1(libraryId))
  }

  /**
   * Fetches the list of Libraries usable with the user's Databricks Cloud account.
   * @return List of Library metadata, i.e. (name, id, folder)
   */
  private[sbtdatabricks] def fetchLibraries: Seq[LibraryListResult] = {
    mapper.readValue[Seq[LibraryListResult]](send(ListLibrariesRequestV1()))
  }

  /**
   * Get the status of the Library
   * @param libraryId the id of the Library
   * @return Information on the status of the Library, which clusters it is attached to,
   *         files, etc...
   */
  private[sbtdatabricks] def getLibraryStatus(libraryId: String): LibraryStatus = {
    mapper.readValue[LibraryStatus](send(GetLibraryStatusRequestV1(libraryId)))
  }

  /**
   * Check whether an older version of the library is attached to the given clusters
   * @param lib the libraries that will be uploaded
   * @param clusters all clusters accessible by the user
   * @param onClusters List of clusters to check whether the libraries are attached to
   * @return List of clusters of interest (supplied by dbcClusters) this library is attached to
   */
  private[sbtdatabricks] def isOldVersionAttached(
      lib: UploadedLibrary,
      clusters: Seq[Cluster],
      onClusters: Iterable[ClusterName]): Iterable[ClusterName] = {
    val status = getLibraryStatus(lib.id)
    val libraryClusterStatusMap = status.statuses.map(s => (s.clusterId, s.status)).toMap
    val clusterList = new ArrayBuffer[ClusterName](onClusters.size)
    foreachCluster(onClusters, clusters) { cluster =>
        libraryClusterStatusMap.get(cluster.id).foreach { state =>
          if (state != "Detached") {
            clusterList.append(cluster.name)
          }
        }
      }
    clusterList.toSet
  }

  /**
   * Delete the given libraries
   * @param libs The libraries to delete
   * @return true that means that the operation completed
   */
  private[sbtdatabricks] def deleteLibraries(libs: Seq[UploadedLibrary]): Boolean = {
    libs.foreach { lib =>
      outputStream.println(s"Deleting older version of ${lib.name}")
      deleteJar(lib.id)
    }
    // We need to have a hack for SBT to handle the operations sequentially. The `true` is to make
    // sure that the function returned a value and the future operations of `deploy` depend on
    // this method
    true
  }

  /**
   * Create an execution context
   * @param language the relevant coding language
   * @param cluster the relevant cluster within which the context will be created
   * @return The id of the execution context
   *
   */
  private[sbtdatabricks] def createContext(
      language: DBCExecutionLanguage,
      cluster: Cluster): ContextId = {
    outputStream.println(
      s"Creating '${language.is}' execution context on cluster '${cluster.name}'")
    mapper.readValue[ContextId](send(CreateContextRequestV1(language.is, cluster.id)))
  }

  /**
   * Check status of an execution context
   * @param contextId Contains the id of the execution context
   * @param cluster the relevant cluster
   * @return status of the execution context
   */
  private[sbtdatabricks] def checkContext(contextId: ContextId, cluster: Cluster): ContextStatus = {
    outputStream.println(s"Checking execution context on cluster '${cluster.name}'")
    val contextStatus =
      mapper.readValue[ContextStatus](send(CheckContextRequestV1(contextId, cluster)))
    outputStream.println(contextStatus.toString)
    contextStatus
  }

  /**
   * Destroy an execution context
   * @param contextId Contains the id of the execution context
   * @param cluster the relevant cluster
   * @return the id of the execution context
   */
  private[sbtdatabricks] def destroyContext(
      contextId: ContextId,
      cluster: Cluster): ContextId = {
    outputStream.println(s"Terminating execution context on cluster '${cluster.name}'")
    mapper.readValue[ContextId](send(DestroyContextRequestV1(contextId.id, cluster.id)))
  }


  /**
   * Issue and execute a command
   * @param language the relevant coding language
   * @param cluster the relevant cluster within which the context will be created
   * @param contextId The id of the execution context
   * @param commandFile The file containing the code to be executed on the cluster
   * @return The id of the command
   *
   */
  private[sbtdatabricks] def executeCommand(
      language: DBCExecutionLanguage,
      cluster: Cluster,
      contextId: ContextId,
      commandFile: File): CommandId = {
    outputStream.println(s"Executing '${language.is}' command on cluster '${cluster.name}'")
    mapper.readValue[CommandId](send(
      ExecuteCommandRequestV1(language.is, cluster.id, contextId.id, commandFile)))
  }

  /**
   * Check the status of a command
   * @param cluster the relevant cluster within which the context will be created
   * @param contextId The id of the execution context
   * @param commandId The id returned for the code to be executed on the cluster
   * @return The status of the command
   *
   */
  private[sbtdatabricks] def checkCommand(
      cluster: Cluster,
      contextId: ContextId,
      commandId: CommandId): CommandStatus = {
    outputStream.println(s"Checking status of command on cluster '${cluster.name}'")
    val commandStatus = mapper.readValue[CommandStatus](
      send(CheckCommandRequestV1(cluster.id, contextId.id, commandId.id)))
    outputStream.println(commandStatus.toString)
    commandStatus
  }

  /**
   * Cancel a command
   * @param cluster the relevant cluster within which the context will be created
   * @param contextId The id of the execution context
   * @param commandId The id returned for the code to be executed on the cluster
   * @return The id of the command
   *
   */
  private[sbtdatabricks] def cancelCommand(
      cluster: Cluster,
      contextId: ContextId,
      commandId: CommandId): CommandId = {
    outputStream.println(s"Cancelling command on cluster '${cluster.name}'")
    mapper.readValue[CommandId](
      send(CancelCommandRequestV1(cluster.id, contextId.id, commandId.id)))
  }

  /**
   * Refactored to take a tuple so that we can reuse foreachCluster.
   * @param library The metadata of the uploaded library
   * @param cluster The cluster to attach the library to
   * @return Response from Databricks Cloud
   */
  private[sbtdatabricks] def attachToCluster(library: UploadedLibrary, cluster: Cluster): String = {
    outputStream.println(s"Attaching ${library.name} to cluster '${cluster.name}'")
    send(LibraryAttachRequestV1(library.id, cluster.id))
  }

  /**
   * Fetch the list of clusters the user has access to
   * @return List of clusters (name, id, status, etc...)
   */
  private[sbtdatabricks] def fetchClusters: Seq[Cluster] = {
    mapper.readValue[Seq[Cluster]](send(ListClustersRequestV1()))
  }

  /**
   * Get detailed information on a cluster
   * @param clusterId the cluster to get detailed information on
   * @return cluster's metadata (name, id, status, etc...)
   */
  private[sbtdatabricks] def clusterInfo(clusterId: String): Cluster = {
    mapper.readValue[Cluster](send(GetClusterStatusRequestV1(clusterId)))
  }

  /** Restart a cluster */
  private[sbtdatabricks] def restartCluster(cluster: Cluster): String = {
    outputStream.println(s"Restarting cluster: ${cluster.name}")
    send(RestartClusterRequestV1(cluster.id))
  }

  /**
   * Helper method to handle cluster related functions,
   * and handle the special 'ALL_CLUSTERS' option.
   * @param onClusters The clusters to invoke the function on
   * @param allClusters The list of all clusters, which the user has access to
   * @param f The function to perform on the cluster
   */
  private[sbtdatabricks] def foreachCluster(
      onClusters: Iterable[String],
      allClusters: Seq[Cluster])(f: Cluster => Unit): Unit = {
    require(onClusters.nonEmpty, "Please specify a cluster.")
    val hasAllClusters = onClusters.find(_ == DBC_ALL_CLUSTERS)
    if (hasAllClusters.isDefined) {
      allClusters.foreach { cluster =>
        f(cluster)
      }
    } else {
      onClusters.foreach { clusterName =>
        val givenCluster = allClusters.find(_.name == clusterName)
        if (givenCluster.isEmpty) {
          throw new NoSuchElementException(s"Cluster with name: $clusterName not found!")
        }
        givenCluster.foreach { cluster =>
          f(cluster)
        }
      }
    }
  }

  /**
   * Returns the response body as a string for HTTP 200 responses, or throws an exception with a
   * useful message for error responses.
   */
  private[this] def send(request: DBApiRequest): String = {
    Try(request.getRequest(client, endpoint).send()) match {
      case Success(response) =>
        if (response.getStatus >= 300) {
          val errorMessage: String = {
            if (response.getContentAsString != null) {
              val stringResponse = response.getContentAsString
              try {
                mapper.readValue[ErrorResponse](stringResponse).error
              } catch {
                case NonFatal(e) => response.getReason
              }
            } else {
              response.getReason
            }
          }
          outputStream.println(s"ERROR: $errorMessage")
          throw new HttpResponseException(errorMessage, response)
        } else {
          response.getContentAsString
        }
      case Failure(e) =>
        outputStream.println(s"ERROR: ${e.getMessage}")
        throw e
    }
  }
}

object DatabricksHttp {

  /** Create an SSL client to handle communication. */
  private[sbtdatabricks] def getApiClient(
      endpoint: String,
      username: String,
      password: String): HttpClient = {

    val ssl = new SslContextFactory(true)
    // TLSv1.2 is only available in Java 7 and above
    ssl.setProtocol("TLSv1.2")
    val index = endpoint.indexOf("/api")
    val strippedEndpoint = if (index < 0) {
      endpoint
    } else {
      endpoint.take(index)
    }
    val client = new HttpClient(ssl)
    client.setFollowRedirects(false)
    client.getAuthenticationStore.addAuthentication(new BasicAuthentication(
      new URI(strippedEndpoint), "DatabricksRealm", username, password))
    client.setUserAgentField(new HttpField(HttpHeader.USER_AGENT, "sbt-databricks"))
    client.start()
    client
  }

  private[sbtdatabricks] def apply(
      endpoint: String,
      username: String,
      password: String): DatabricksHttp = {
    val cli = DatabricksHttp.getApiClient(endpoint, username, password)
    new DatabricksHttp(endpoint, cli)
  }

  /** Returns a mock testClient */
  def testClient(client: HttpClient, file: File): DatabricksHttp = {
    val outputFile = new PrintStream(file)
    new DatabricksHttp("test", client, outputFile)
  }

  private[sbtdatabricks] val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
}

// exposed for tests
object DBApiEndpoints {

  final val CLUSTER_LIST = "/clusters/list"
  final val CLUSTER_RESTART = "/clusters/restart"

  final val CONTEXT_CREATE = "/contexts/create"
  final val CONTEXT_STATUS = "/contexts/status"
  final val CONTEXT_DESTROY = "/contexts/destroy"

  final val COMMAND_EXECUTE = "/commands/execute"
  final val COMMAND_CANCEL = "/commands/cancel"
  final val COMMAND_STATUS = "/commands/status"

  final val LIBRARY_LIST = "/libraries/list"
  final val LIBRARY_UPLOAD = "/libraries/upload"

  final val FILE_DOWNLOAD = "/files/download"

  final val LIBRARY_ATTACH = "/libraries/attach"
  final val LIBRARY_DETACH = "/libraries/detach"
  final val LIBRARY_DELETE = "/libraries/delete"
  final val LIBRARY_STATUS = "/libraries/status"

  final val CLUSTER_INFO = "/clusters/status"
  final val CLUSTER_CREATE = "/clusters/create"
  final val CLUSTER_RESIZE = "/clusters/resize"
  final val CLUSTER_DELETE = "/clusters/delete"
}
