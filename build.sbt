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

lazy val root = (project in file("."))
  .aggregate(async)
