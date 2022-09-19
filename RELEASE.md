Deploying snapshots and releases uses the same command-line:

`mvn --batch-mode -e -V -U -Dsurefire.useFile=false -DstagingProfileId=3799bb102c7f24 -Ddeploy clean deploy`

The only difference is the `pom.xml` version number. If the version is a snapshot, then a snapshot is
deployed. If it is a release, then a release is deployed.

# Release steps

* Remove `-SNAPSHOT` from the `pom.xml` version
* Commit `Released version X.Y`
* Create tag `release-X.Y`
* Deploy to maven-central using the above command-line.
* Change the version number to the next SNAPSHOT version.
* Copy the Javadoc from `/target/site/apidocs` to a temporary directory.
* Check out the `gh-pages` branch.
* Create a new directory for the release (e.g. `/X.Y/api/docs`) and copy the release Javadoc into it.
* Commit the changes and switch back to the `master` branch.