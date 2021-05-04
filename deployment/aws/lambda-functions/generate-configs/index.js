var response = require('cfn-response');

exports.handler = function (event, context) {
    const fs = require('fs');
    fs.mkdirSync('/mnt/share/domain1');
    fs.mkdirSync('/mnt/share/domain2');
    fs.mkdirSync('/mnt/share/participant1');
    fs.mkdirSync('/mnt/share/participant2');
    fs.mkdirSync('/mnt/share/participant3');


    let DomainOne = `canton {
    domains {
        domain1 {
            storage {
                type = postgres
                config {
                    url = "jdbc:postgresql://${process.env.RdsHost}:5432/domain1"
                    user = domain1
                    password = ${process.env.DBPasswordDomainOne}
                }
            }

            public-api {
                address = 0.0.0.0
                port = 3000
            }

            admin-api {
                address = 0.0.0.0
                port = 3001
            }
        }
    }
}`;
    fs.writeFileSync('/mnt/share/domain1/canton.conf', DomainOne);

    let DomainTwo = `canton {
    domains {
        domain2 {
            storage {
                type = postgres
                config {
                    url = "jdbc:postgresql://${process.env.RdsHost}:5432/domain2"
                    user = domain2
                    password = ${process.env.DBPasswordDomainTwo}
                }
            }

            public-api {
                address = 0.0.0.0
                port = 3010
            }

            admin-api {
                address = 0.0.0.0
                port = 3011
            }
        }
    }
}`;
    fs.writeFileSync('/mnt/share/domain2/canton.conf', DomainTwo);

    let participant1 = `canton {
    participants {
        participant1 {
            storage {
                type = postgres
                config {
                    url = "jdbc:postgresql://${process.env.RdsHost}:5432/participant1"
                    user = participant1
                    password = ${process.env.DBPasswordParticipantOne}
                }
            }

            ledger-api {
                address = 0.0.0.0
                port = 4000
            }

            admin-api {
                address = 0.0.0.0
                port = 4001
            }
        }
    }
}`;
    fs.writeFileSync('/mnt/share/participant1/canton.conf', participant1);

    let participant2 = `canton {
    participants {
        participant2 {
            storage {
                type = postgres
                config {
                    url = "jdbc:postgresql://${process.env.RdsHost}:5432/participant2"
                    user = participant2
                    password = ${process.env.DBPasswordParticipantTwo}
                }
            }

            ledger-api {
                address = 0.0.0.0
                port = 4010
            }

            admin-api {
                address = 0.0.0.0
                port = 4011
            }
        }
    }
}`;
    fs.writeFileSync('/mnt/share/participant2/canton.conf', participant2);

    let participant3 = `canton {
    participants {
        participant3 {
            storage {
                type = postgres
                config {
                    url = "jdbc:postgresql://${process.env.RdsHost}:5432/participant3"
                    user = participant3
                    password = ${process.env.DBPasswordParticipantThree}
                }
            }

            ledger-api {
                address = 0.0.0.0
                port = 4020
            }

            admin-api {
                address = 0.0.0.0
                port = 4021
            }
        }
    }
}`;
    fs.writeFileSync('/mnt/share/participant3/canton.conf', participant3);

    let participant1Navigator = `users {
        "participant1" {
            party="participant1"
        }
}`;
    fs.writeFileSync('/mnt/share/participant1/ui-backend.conf', participant1Navigator);

    let participant2Navigator = `users {
        "participant2" {
            party="participant2"
        }
}`;
    fs.writeFileSync('/mnt/share/participant2/ui-backend.conf', participant2Navigator);

    let participant3Navigator = `users {
        "participant3" {
            party="participant3"
        }
}`;
    fs.writeFileSync('/mnt/share/participant3/ui-backend.conf', participant3Navigator);

    let participant1Bootstrap = `if (participant1.domains.list_registered().isEmpty) {
    participant1.domains.connect("domain1", "http://domain1.canton.io:3000")
}`;
    fs.writeFileSync('/mnt/share/participant1/bootstrap.canton', participant1Bootstrap);

    let participant2Bootstrap = `if (participant2.domains.list_registered().isEmpty) {
    participant2.domains.connect("domain1", "http://domain1.canton.io:3000")
    participant2.domains.connect("domain2", "http://domain2.canton.io:3010")
}`;
    fs.writeFileSync('/mnt/share/participant2/bootstrap.canton', participant2Bootstrap);

    let participant3Bootstrap = `if (participant3.domains.list_registered().isEmpty) {
    participant3.domains.connect("domain2", "http://domain2.canton.io:3010")
}`;
    fs.writeFileSync('/mnt/share/participant3/bootstrap.canton', participant3Bootstrap);

    var responseData = {};
    responseData = { "efs": "All configuration is created!" };
    response.send(event, context, response.SUCCESS, responseData);
};
