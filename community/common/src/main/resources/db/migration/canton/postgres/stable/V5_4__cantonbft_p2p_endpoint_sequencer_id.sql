alter table ord_p2p_endpoints add column node_id varchar collate "C" default null;

create or replace view debug.ord_p2p_endpoints as
  select
    address,
    port,
    transport_security,
    custom_server_trust_certificates,
    client_certificate_chain,
    client_private_key_file,
    node_id
  from ord_p2p_endpoints;
