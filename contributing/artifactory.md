# Access to Artifactory

The Canton Enterprise docker images are hosted on [Artifactory](https://digitalasset.jfrog.io).
During daily work, engineers do not need access to Artifactory.
In case you need access nevertheless, put your personal artifactory credentials in the `~/.netrc` file:

```
machine digitalasset.jfrog.io
    login <user>
    password <identity-token>
```

As an alternative, the credentials can be extracted from the following environment variables:

```
ARTIFACTORY_USER
ARTIFACTORY_PASSWORD
```

(*Hint*: you may export these from your `.envrc.private` file in the canton directory)

To obtain `<user>` and `<identity-token>` follow these steps:

- Navigate your browser to https://digitalasset.jfrog.io and log in using Google-SSO
- If you don't have access, contact help@digitalasset.com.
- Click in the top-right corner under "Welcome, ..." and select "Edit Profile"
- `<user>` is the string following "User Profile: " near the top (typically your firstname.lastname)
- Click on "Generate an Identity Token" at the top
- Give it a description, such as "Canton Enterprise build"
- The newly generated token is shown in the text box below "Reference Token". Click on the "Copy to Clipboard" icon to fetch the identity token, then paste it as your "password" credential. (Note that once you close the window, **the token will no longer be accessible from Artifactory**.)
- The generated token will be valid for about a year. A new one will need to be generated after that for continued access.
- To test your setup, run the following and check that the return code is "200" (OK):

```
curl --netrc -sSL -o /dev/null -w "%{http_code}\n" https://digitalasset.jfrog.io/artifactory/assembly
```
