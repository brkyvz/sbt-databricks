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

import scala.collection.JavaConversions._

import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpRequestBase}
import org.apache.http.client.utils.URLEncodedUtils
import org.apache.http.entity.StringEntity
import org.apache.http.entity.mime.MultipartEntity
import org.apache.http.entity.mime.content.{FileBody, StringBody}
import org.apache.http.message.BasicNameValuePair

import sbtdatabricks.{Cluster, ContextId, DatabricksHttp, DBApiEndpoints}

// scalastyle:off
private[sbtdatabricks] object requests {
// scalastyle:on
  import DBApiEndpoints._
  import DatabricksHttp.mapper

  sealed trait DBApiRequest {
    def apiVersion: String

    final def getRequest(baseEndpoint: String): HttpRequestBase = {
      getRequestInternal(getApiUrl(baseEndpoint))
    }

    protected def getRequestInternal(endpoint: String): HttpRequestBase

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
    override def getRequestInternal(endpoint: String): HttpRequestBase = {
      setJsonRequest(this, new HttpPost(endpoint + CONTEXT_CREATE))
    }
  }

  /** Request sent to create a Spark Context */
  case class CheckContextRequestV1(contextId: ContextId, cluster: Cluster) extends DBApiV1Request {
    override def getRequestInternal(endpoint: String): HttpRequestBase = {
      val form = URLEncodedUtils.format(List(new BasicNameValuePair("clusterId", cluster.id),
        new BasicNameValuePair("contextId", contextId.id)), "utf-8")
      new HttpGet(endpoint + CONTEXT_STATUS + "?" + form)
    }
  }

  /** Request sent to destroy a Spark Context */
  case class DestroyContextRequestV1(clusterId: String, contextId: String) extends DBApiV1Request {
    override def getRequestInternal(endpoint: String): HttpRequestBase = {
      setJsonRequest(this, new HttpPost(endpoint + CONTEXT_DESTROY))
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
    override def getRequestInternal(endpoint: String): HttpRequestBase = {
      setJsonRequest(this, new HttpPost(endpoint + COMMAND_CANCEL))
    }
  }

  /** Request sent to check the status of a command */
  case class CheckCommandRequestV1(
      clusterId: String,
      contextId: String,
      commandId: String) extends DBApiV1Request {
    override def getRequestInternal(endpoint: String): HttpRequestBase = {
      val form = URLEncodedUtils.format(List(new BasicNameValuePair("clusterId", clusterId),
        new BasicNameValuePair("contextId", contextId),
        new BasicNameValuePair("commandId", commandId)), "utf-8")
      new HttpGet(endpoint + COMMAND_STATUS + "?" + form)
    }
  }

  /** Request sent to check the status of a command */
  case class ExecuteCommandRequestV1(
      language: String,
      clusterId: String,
      contextId: String,
      commandFile: File) extends DBApiV1Request {
    override def getRequestInternal(endpoint: String): HttpRequestBase = {
      val post = new HttpPost(endpoint + COMMAND_EXECUTE)
      val entity = new MultipartEntity()
      entity.addPart("language", new StringBody(language))
      entity.addPart("clusterId", new StringBody(clusterId))
      entity.addPart("contextId", new StringBody(contextId))
      entity.addPart("command", new FileBody(commandFile))
      post.setEntity(entity)
      post
    }
  }

  ///////////////////////////////////////////
  // Library Requests
  ///////////////////////////////////////////

  /** Request sent to attach a library to a cluster */
  case class LibraryAttachRequestV1(libraryId: String, clusterId: String) extends DBApiV1Request {
    override def getRequestInternal(endpoint: String): HttpRequestBase = {
      setJsonRequest(this, new HttpPost(endpoint + LIBRARY_ATTACH))
    }
  }

  case class DeleteLibraryRequestV1(libraryId: String) extends DBApiV1Request {
    override def getRequestInternal(endpoint: String): HttpRequestBase = {
      val post = new HttpPost(endpoint + LIBRARY_DELETE)
      val form = List(new BasicNameValuePair("libraryId", libraryId))
      post.setEntity(new UrlEncodedFormEntity(form))
      post
    }
  }

  case class ListLibrariesRequestV1() extends DBApiV1Request {
    override def getRequestInternal(endpoint: String): HttpRequestBase = {
      new HttpGet(endpoint + LIBRARY_LIST)
    }
  }

  case class GetLibraryStatusRequestV1(libraryId: String) extends DBApiV1Request {
    override def getRequestInternal(endpoint: String): HttpRequestBase = {
      val form =
        URLEncodedUtils.format(List(new BasicNameValuePair("libraryId", libraryId)), "utf-8")
      new HttpGet(endpoint + LIBRARY_STATUS + "?" + form)
    }
  }

  case class UploadLibraryRequest(path: String, file: File) extends DBApiV2Request {
    override def getRequestInternal(endpoint: String): HttpRequestBase = {
      val post = new HttpPost(endpoint + DBFS_PUT)
      val entity = new MultipartEntity()
      entity.addPart("path", new StringBody(path))
      entity.addPart("overwrite", new StringBody("true"))
      entity.addPart("contents", new FileBody(file))
      post.setEntity(entity)
      post
    }
  }

  ///////////////////////////////////////////
  // Cluster Requests
  ///////////////////////////////////////////

  /** Request sent to restart a cluster */
  case class RestartClusterRequestV1(clusterId: String) extends DBApiV1Request {
    override def getRequestInternal(endpoint: String): HttpRequestBase = {
      setJsonRequest(this, new HttpPost(endpoint + CLUSTER_RESTART))
    }
  }

  /** Request sent to get the status of a cluster */
  case class GetClusterStatusRequestV1(clusterId: String) extends DBApiV1Request {
    override def getRequestInternal(endpoint: String): HttpRequestBase = {
      val form =
        URLEncodedUtils.format(List(new BasicNameValuePair("clusterId", clusterId)), "utf-8")
      new HttpGet(endpoint + CLUSTER_INFO + "?" + form)
    }
  }

  /** Request sent to get the status of a cluster */
  case class ListClustersRequestV1() extends DBApiV1Request {
    override def getRequestInternal(endpoint: String): HttpRequestBase = {
      new HttpGet(endpoint + CLUSTER_LIST)
    }
  }

  private[this] def setJsonRequest(contents: DBApiRequest, post: HttpPost): HttpPost = {
    val form = new StringEntity(mapper.writeValueAsString(contents))
    form.setContentType("application/json")
    post.setEntity(form)
    post
  }
}
