ThisBuild / versionPolicyPreviousVersionRepositories := SbtResolvers(Set(codeArtifactRepo("maven-jvm").value))
ThisBuild / version ~= (_.replace("-SNAPSHOT", ""))
ThisBuild / dynver ~= (_.replace("-SNAPSHOT", ""))
