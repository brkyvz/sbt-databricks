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

package sbtdatabricks.util

import java.io.File
import java.net.{URI, HttpCookie}
import java.nio.charset.StandardCharsets
import java.nio.file.{Paths, Path}
import java.util
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions._

import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.client.api._
import org.eclipse.jetty.client.api.Request.{CommitListener, QueuedListener, RequestListener, BeginListener}
import org.eclipse.jetty.client.api.Response._
import org.eclipse.jetty.client.util.{PathContentProvider, MultiPartContentProvider, StringContentProvider}
import org.eclipse.jetty.http.{HttpMethod, HttpFields, HttpHeader, HttpVersion}
import org.eclipse.jetty.util.{MultiPartWriter, Fields}

import sbtdatabricks.{Cluster, ContextId, DatabricksHttp, DBApiEndpoints}

// scalastyle:off
private[sbtdatabricks] object requests {
// scalastyle:on
  import DBApiEndpoints._
  import DatabricksHttp.mapper

  sealed trait DBApiRequest {
    def apiVersion: String

    final def getRequest(client: HttpClient, baseEndpoint: String): Request = {
      getRequestInternal(client, getApiUrl(baseEndpoint))
    }

    protected def getRequestInternal(client: HttpClient, endpoint: String): Request

    private[this] def getApiUrl(endpoint: String): String = {
      val untilApi = endpoint.indexOf("/api")
      if (untilApi < 0) {
        // url provided without /api/$apiVersion
        endpoint.stripSuffix("/") + "/api/" + apiVersion
      } else {
        endpoint.take(untilApi) + "/api/" + apiVersion
      }
    }
  }

  sealed trait DBApiV1Request extends DBApiRequest {
    override def apiVersion: String = "1.2"
  }

  sealed trait DBApiV2Request extends DBApiRequest {
    override def apiVersion: String = "2.0"
  }

  ///////////////////////////////////////////
  // Execution Context Requests
  ///////////////////////////////////////////

  /** Request sent to create a Spark Context */
  case class CreateContextRequestV1(language: String, clusterId: String) extends DBApiV1Request {
    override def getRequestInternal(client: HttpClient, endpoint: String): Request = {
      setJsonRequest(this, client.POST(endpoint + CONTEXT_CREATE))
    }
  }

  /** Request sent to create a Spark Context */
  case class CheckContextRequestV1(contextId: ContextId, cluster: Cluster) extends DBApiV1Request {
    override def getRequestInternal(client: HttpClient, endpoint: String): Request = {
      client.newRequest(endpoint + CONTEXT_STATUS)
        .param("clusterId", cluster.id)
        .param("contextId", contextId.id)
    }
  }

  /** Request sent to destroy a Spark Context */
  case class DestroyContextRequestV1(clusterId: String, contextId: String) extends DBApiV1Request {
    override def getRequestInternal(client: HttpClient, endpoint: String): Request = {
      setJsonRequest(this, client.POST(endpoint + CONTEXT_DESTROY))
    }
  }

  ///////////////////////////////////////////
  // Command Related Requests
  ///////////////////////////////////////////

  /** Request sent to cancel a command */
  case class CancelCommandRequestV1(
      clusterId: String,
      contextId: String,
      commandId: String) extends DBApiV1Request {
    override def getRequestInternal(client: HttpClient, endpoint: String): Request = {
      setJsonRequest(this, client.POST(endpoint + COMMAND_CANCEL))
    }
  }

  /** Request sent to check the status of a command */
  case class CheckCommandRequestV1(
      clusterId: String,
      contextId: String,
      commandId: String) extends DBApiV1Request {
    override def getRequestInternal(client: HttpClient, endpoint: String): Request = {
      client.newRequest(endpoint + COMMAND_STATUS)
        .param("clusterId", clusterId)
        .param("contextId", contextId)
        .param("commandId", commandId)
    }
  }

  /** Request sent to check the status of a command */
  case class ExecuteCommandRequestV1(
      language: String,
      clusterId: String,
      contextId: String,
      commandFile: File) extends DBApiV1Request {
    override def getRequestInternal(client: HttpClient, endpoint: String): Request = {
      val form = new MultiPartContentProvider()
      form.addFieldPart("language", new StringContentProvider(language), null)
      form.addFieldPart("clusterId", new StringContentProvider(clusterId), null)
      form.addFieldPart("contextId", new StringContentProvider(contextId), null)
      form.addFilePart("command", null,
        new PathContentProvider(Paths.get(commandFile.getAbsolutePath)), null)
      client.POST(endpoint + COMMAND_EXECUTE)
        .content(form)
    }
  }

  ///////////////////////////////////////////
  // Library Requests
  ///////////////////////////////////////////

  /** Request sent to attach a library to a cluster */
  case class LibraryAttachRequestV1(libraryId: String, clusterId: String) extends DBApiV1Request {
    override def getRequestInternal(client: HttpClient, endpoint: String): Request = {
      setJsonRequest(this, client.POST(endpoint + LIBRARY_ATTACH))
    }
  }

  case class DeleteLibraryRequestV1(libraryId: String) extends DBApiV1Request {
    override def getRequestInternal(client: HttpClient, endpoint: String): Request = {
      client.POST(endpoint + LIBRARY_DELETE)
        .param("libraryId", libraryId)
    }
  }

  case class ListLibrariesRequestV1() extends DBApiV1Request {
    override def getRequestInternal(client: HttpClient, endpoint: String): Request = {
      client.newRequest(endpoint + LIBRARY_LIST)
    }
  }

  case class GetLibraryStatusRequestV1(libraryId: String) extends DBApiV1Request {
    override def getRequestInternal(client: HttpClient, endpoint: String): Request = {
      client.newRequest(endpoint + LIBRARY_STATUS)
        .param("libraryId", libraryId)
    }
  }

  case class UploadLibraryRequest(name: String, file: File, folder: String) extends DBApiV1Request {
    override def getRequestInternal(client: HttpClient, endpoint: String): Request = {
      val form = new MultiPartContentProvider()
      form.addFieldPart("name", new StringContentProvider(name), null)
      form.addFieldPart("libType", new StringContentProvider("scala"), null)
      form.addFieldPart("folder", new StringContentProvider(folder), null)
      form.addFilePart("uri", file.getName,
        new PathContentProvider(Paths.get(file.getAbsolutePath)), null)
      client.POST(endpoint + LIBRARY_UPLOAD)
        .content(form)
    }
  }

  ///////////////////////////////////////////
  // Cluster Requests
  ///////////////////////////////////////////

  /** Request sent to restart a cluster */
  case class RestartClusterRequestV1(clusterId: String) extends DBApiV1Request {
    override def getRequestInternal(client: HttpClient, endpoint: String): Request = {
      setJsonRequest(this, client.POST(endpoint + CLUSTER_RESTART))
    }
  }

  /** Request sent to get the status of a cluster */
  case class GetClusterStatusRequestV1(clusterId: String) extends DBApiV1Request {
    override def getRequestInternal(client: HttpClient, endpoint: String): Request = {
      client.newRequest(endpoint + CLUSTER_INFO)
        .param("clusterId", clusterId)
    }
  }

  /** Request sent to get the status of a cluster */
  case class ListClustersRequestV1() extends DBApiV1Request {
    override def getRequestInternal(client: HttpClient, endpoint: String): Request = {
      client.newRequest(endpoint + CLUSTER_LIST)
    }
  }

  private[this] def setJsonRequest(contents: DBApiRequest, post: Request): Request = {
    post.content(new StringContentProvider("application/json",
      mapper.writeValueAsString(contents), StandardCharsets.UTF_8))
  }
}
