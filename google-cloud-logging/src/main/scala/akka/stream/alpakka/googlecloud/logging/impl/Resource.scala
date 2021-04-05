/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging.impl

import akka.annotation.InternalApi
import akka.dispatch.{Dispatchers, ExecutionContexts}
import akka.stream.Materializer
import akka.stream.alpakka.google.ComputeMetadata
import akka.util.Helpers

import scala.concurrent.Future
import scala.io.{Codec, Source}
import scala.util.{Success, Try}

// Adapted from com.google.cloud.logging.MonitoredResourceUtil
@InternalApi
private[logging] object Resource {

  def apply(`type`: String): Resource =
    Array(CloudRun, GaeApp, GceInstance, K8sContainer, Global)
      .find(_.`type` == Helpers.toRootLowerCase(`type`))
      .getOrElse(Other(`type`))

  def autoDetect(implicit mat: Materializer): Future[Resource] =
    if (sys.env.contains("K_SERVICE")
        && sys.env.contains("K_REVISION")
        && sys.env.contains("K_CONFIGURATION")
        && !sys.env.contains("KUBERNETES_SERVICE_HOST"))
      Future.successful(CloudRun)
    else if (sys.env.contains("GAE_INSTANCE"))
      Future.successful(GaeApp)
    else if (sys.env.contains("KUBERNETES_SERVICE_HOST"))
      Future.successful(K8sContainer)
    else if (sys.props.contains("com.google.appengine.application.id"))
      Future.successful(GaeApp)
    else
      ComputeMetadata.getInstanceId
        .map { _ =>
          GceInstance
        }(ExecutionContexts.parasitic)
        .recover { case _ => Global }(ExecutionContexts.parasitic)

}

@InternalApi
private[logging] sealed abstract class Resource(val `type`: String, _labels: Label*) {
  final def labels(implicit mat: Materializer): Future[Map[String, String]] = {
    import mat.executionContext
    Future
      .traverse(_labels) { label =>
        label.value.map(_.map(v => (label.key, v)))
      }
      .map(_.flatten.toMap)
  }
}

private case object CloudRun extends Resource("cloud_run_revision", RevisionName, ServiceName, Location)
private case object GaeApp extends Resource("gae_app", ModuleId, VersionId, Zone)
private case object GceInstance extends Resource("gce_instance", InstanceId, Zone)
private case object K8sContainer
    extends Resource("k8s_container", Location, ClusterName, NamespaceName, PodName, ContainerName)
private case object Global extends Resource("global")
private case class Other(override val `type`: String) extends Resource(`type`)

private sealed abstract class Label(final val key: String) {
  def value(implicit mat: Materializer): Future[Option[String]]
}

private sealed abstract class EnvLabel(key: String, env: String) extends Label(key) {
  final def value(implicit mat: Materializer) = Future.successful(sys.env.get(env))
}

private sealed abstract class PropertyLabel(key: String, prop: String) extends Label(key) {
  final def value(implicit mat: Materializer) = Future.successful(sys.props.get(prop))
}

private sealed abstract class FutureLabel(key: String) extends Label(key) {
  final def value(implicit mat: Materializer) =
    get.transform { t =>
      Success(t.toOption)
    }(ExecutionContexts.parasitic)
  protected def get(implicit mat: Materializer): Future[String]
}

private case object AppId extends PropertyLabel("app_id", "com.google.appengine.application.id")

private case object ClusterName extends FutureLabel("cluster_name") {
  override def get(implicit mat: Materializer) = ComputeMetadata.getClusterName
}

private case object K8sContainerName extends Label("container_name") {
  override def value(implicit mat: Materializer) = Future.successful {
    sys.env.get("HOSTNAME").map { hostname =>
      hostname.substring(0, hostname.indexOf("-"))
    }
  }
}

private case object ContainerName extends FutureLabel("container_name") {
  override protected def get(implicit mat: Materializer) = ComputeMetadata.getContainerName
}

private case object InstanceId extends FutureLabel("instance_id") {
  override protected def get(implicit mat: Materializer) = ComputeMetadata.getInstanceId
}

private case object InstanceName extends EnvLabel("instance_name", "GAE_INSTANCE")

private case object Location extends FutureLabel("location") {
  override def get(implicit mat: Materializer) =
    ComputeMetadata.getZone.map { zone =>
      if (zone.endsWith("-1"))
        zone.substring(0, zone.length - 2)
      else
        zone
    }(mat.executionContext)
}

private case object ModuleId extends EnvLabel("module_id", "GAE_SERVICE")

private case object NamespaceId extends FutureLabel("namespace_id") {
  override def get(implicit mat: Materializer) = ComputeMetadata.getNamespaceId
}

private case object NamespaceName extends Label("namespace_name") {
  override def value(implicit mat: Materializer) = {
    implicit val ec = mat.system.dispatchers.lookup(Dispatchers.DefaultBlockingDispatcherId)
    val filePath =
      sys.env.getOrElse("KUBERNETES_NAMESPACE_FILE", "/var/run/secrets/kubernetes.io/serviceaccount/namespace")
    Future {
      val src = Source.fromFile(filePath)(Codec.UTF8)
      val namespace = Try(src.getLines.mkString("\n"))
      src.close()
      namespace.toOption
    }
  }
}

private case object PodName extends EnvLabel("pod_id", "HOSTNAME")

private case object RevisionName extends EnvLabel("revision_name", "K_REVISION")

private case object ServiceName extends EnvLabel("service_name", "K_SERVICE")

private case object VersionId extends EnvLabel("version_id", "GAE_VERSION")

private case object Zone extends FutureLabel("zone") {
  override def get(implicit mat: Materializer) = ComputeMetadata.getZone
}
