const { Pool, Client } = require('pg');
const { send, SUCCESS } = require('cfn-response-async');

exports.handler = async (event, context) => {
  let MasterUser = process.env.MasterUser;
  let DBPrefix = process.env.DBPrefix;
  let RdsHost = process.env.RdsHost;
  let MasterPassword = process.env.MasterPassword;
  let DBPasswordDomainOne = process.env.DBPasswordDomainOne;
  let DBPasswordDomainTwo = process.env.DBPasswordDomainTwo;
  let DBPasswordParticipantOne = process.env.DBPasswordParticipantOne;
  let DBPasswordParticipantTwo = process.env.DBPasswordParticipantTwo;
  let DBPasswordParticipantThree = process.env.DBPasswordParticipantThree;

  const client = new Client({
    user: MasterUser,
    host: RdsHost,
    database: 'MasterDb',
    password: MasterPassword,
    port: 5432,
  });
  client.connect();

  await client.query('CREATE DATABASE domain1');
  await client.query('CREATE DATABASE domain2');
  await client.query('CREATE DATABASE participant1');
  await client.query('CREATE DATABASE participant2');
  await client.query('CREATE DATABASE participant3');

  const domain1 = `create user domain1 with password '${DBPasswordDomainOne}'`;
  await client.query(domain1);
  const domain2 = `create user domain2 with password '${DBPasswordDomainTwo}'`;
  await client.query(domain2);
  const participant1 = `create user participant1 with password '${DBPasswordParticipantOne}'`;
  await client.query(participant1);
  const participant2 = `create user participant2 with password '${DBPasswordParticipantTwo}'`;
  await client.query(participant2);
  const participant3 = `create user participant3 with password '${DBPasswordParticipantThree}'`;
  await client.query(participant3);

  await client.query('grant all privileges on database domain1 to domain1');
  await client.query('grant all privileges on database domain2 to domain2');
  await client.query('grant all privileges on database participant1 to participant1');
  await client.query('grant all privileges on database participant2 to participant2');
  await client.query('grant all privileges on database participant3 to participant3');

  client.end();
  await send(event, context, SUCCESS, {
    Response: `All Databases and users is created!`
  });
}

