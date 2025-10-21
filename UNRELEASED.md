# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE. You can download the Daml Open Source edition from the Daml Connect [Github Release Section](https://github.com/digital-asset/daml/releases/tag/vCANTON_VERSION). The Enterprise edition is available on [Artifactory](https://digitalasset.jfrog.io/artifactory/canton-enterprise/canton-enterprise-CANTON_VERSION.zip).
Please also consult the [full documentation of this release](https://docs.daml.com/CANTON_VERSION/canton/about.html).

INFO: Note that the **"## Until YYYY-MM-DD (Exclusive)" headers**
below should all be Wednesdays to align with the weekly release
schedule, i.e. if you add an entry effective at or after the first
header, prepend the new date header that corresponds to the
Wednesday after your change.

## Until 2025-10-22 (Exclusive)
- **BREAKING** The default security configuration of the APIs has been tightened
    - Maximum token lifetime accepted by the Ledger API (gRPC and JSON) and Admin API has been reduced. Tokens which have
      time to live longer than 5 minutes will not be accepted. This default value can be overridden through
      ```
      canton.participants.<participant>.ledger-api.max-token-lifetime=10.minutes
      ```
      and
      ```
      canton.participants.<participant>.admin-api.max-token-lifetime=10.minutes
      ```
    - The JWKS cache expiration has been reduced to 5 minutes. A JWKS key will be evicted from the cache after that time and
      the participant will access the JWKS address again to pull the fresh keys. You can modify this behavior by setting
      ```
      canton.participants.<participant>.ledger-api.jwks-cache-config.cache-expiration=10.minutes
      ```
      and
      ```
      canton.participants.<participant>.admin-api.jwks-cache-config.cache-expiration=10.minutes
      ```
    - By default, the Canton Admin Tokens no longer entitle the bearer to operate as a `ParticipantAdmin` nor to `ActAs` any party.
      This behavior can be altered by setting
      ```
      canton.participants.<participant>.ledger-api.admin-token-config.act-as-any-party-claim=true
      canton.participants.<participant>.ledger-api.admin-token-config.admin-claim=true
      ```
- **BREAKING**: Ports for (admin, gRPC Ledger, JSON Ledger, public) API have to be provided explicitly
  (they are not generated automatically anymore.)
- ParticipantRepairService.ExportAcsOld and ImportAcsOld are deprecated
- Participant background pruning now motivates Postgres to use the partial pruning index on the
  `par_active_contracts` table. The newly introduced
  `canton.participants.<participant>.parameters.stores.journal-pruning.max-items-expected-to-prune-per-batch`
  config can be adapted in case the pruning index is not used in some cases.
