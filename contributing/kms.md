Running local KMS tests
=======================

Some users do not want to store private keys in the database.
For such users, Canton provides either a configurable encrypted private store using a Key Management Service (KMS),
or it can use this service directly to create and manage its private keys.

To run tests locally using AWS KMS (e.g. `AwsKmsCryptoWithPreDefinedKeysIntegrationTestPostgres`) you need
to set up AWS credentials on your local environment. The easiest way
to do this is to use AWS-Google-Auth tool to create temporary security credentials (sts).
A comprehensive guide on setting up AWS credentials with the `aws-google-auth` (included in the nix dev env) can be found on this
[page](https://digitalasset.atlassian.net/wiki/spaces/DEVSECOPS/pages/719750045/AWS+Google+Auth).
You should also request Canton IT to enable your user account in AWS with the following roles: `da_sso_poweruser_kms`, `da_sso_kms_testing`, `da_sso_canton_kms_test`.

Some users have had problems getting `aws-google-auth` to accept their credentials and have turned
to [gsts](https://github.com/ruimarinho/gsts) instead to get the job done. Unfortunately this is
only available via `npm` and not easily added to the nix env.

To run tests locally using GCP KMS (e.g. `GcpKmsCryptoWithPreDefinedKeysIntegrationTestPostgres`) you
need to set up GCP credentials on your local environment. The easiest way to do this is to use `gcloud auth`
tool, included in the nix dev env, to create a set of default security credentials that will be stored in
`~/.config/gcloud/application_default_credentials.json`. For more information, you can refer to this
[page](https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login).
You should also request Canton IT to enable your user account in GCP so that you can access the `gcp-kms-testing` project.
