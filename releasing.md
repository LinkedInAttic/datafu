# How To Release

We use Sonatype to release artifacts.  Information on how this is set up can be found [here](https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide).  Most of this has already been set up with the `build.xml` file.  You will however need a Sonatype account and must create a Maven `~/.m2/settings.xml` with your account information, as described [here](https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide#SonatypeOSSMavenRepositoryUsageGuide-7a.1.POMandsettingsconfig).

Create the settings.xml from our template:

    mkdir ~/.m2
    cp settings.xml.template ~/.m2/settings.xml

Then edit `~/.m2/settings.xml` and add your user name and password.

We use `gpg` to sign the artifacts, so you'll need `gpg` set up as well. Information on generating PGP signatures with `gpg` can be found [here](https://docs.sonatype.org/display/Repository/How+To+Generate+PGP+Signatures+With+Maven).  Make sure you follow the section *Deleting a Sub Key*.

First run the tests to make sure all is well:

    ant test

If it succeeds, build artifacts and upload to sonatype:

    ant deploy

Login to [Sonatype](https://oss.sonatype.org/index.html#stagingRepositories).  In Staging Repositories you should see a repository for the articacts just uploaded.  Select the repository and click Close.

Now that the repository is closed you can download the artifacts and do some manual testing if you would like before doing a final release.   If you find a problem you can find drop the release by selecting the repository and clicking Drop.  Else to release select the repository and click Release.