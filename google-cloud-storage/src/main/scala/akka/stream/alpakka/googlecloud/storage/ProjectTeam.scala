/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

final class ProjectTeam private (projectNumber: String, team: String) {
  def withProjectNumber(projectNumber: String): ProjectTeam =
    copy(projectNumber = projectNumber)
  def withTeam(team: String): ProjectTeam = copy(team = team)

  private def copy(projectNumber: String = projectNumber, team: String = team): ProjectTeam =
    new ProjectTeam(projectNumber, team)

  override def toString: String =
    s"ProjectTeam(projectNumber=$projectNumber, team=$team)"
}

object ProjectTeam {
  def apply(projectNumber: String, team: String): ProjectTeam =
    new ProjectTeam(projectNumber, team)

  def create(projectNumber: String, team: String): ProjectTeam =
    new ProjectTeam(projectNumber, team)
}
