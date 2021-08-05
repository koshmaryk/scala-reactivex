name := "scala-reactivex"
scalaVersion := "2.13.1"
scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-encoding", "UTF-8",
  "-unchecked",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-value-discard",
  "-Xfuture",
  "-Xexperimental"
)

lazy val async = project
lazy val actorbintree = project
lazy val kvstore = project
lazy val protocols = project
lazy val streaming = project

lazy val root = (project in file("."))
  .aggregate(async)
  .aggregate(actorbintree)
  .aggregate(kvstore)
  .aggregate(protocols)
  .aggregate(streaming)
