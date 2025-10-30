# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from docutils import nodes
from docutils.parsers.rst import Directive

class EnterpriseOnlyDirective(Directive):
    def run(self):
        para_node = nodes.paragraph()
        ref_node = nodes.reference('Daml_Enterprise_license', 'Daml Enterprise license', internal=True, refuri="/canton/usermanual/downloading.html")
        para_node += ref_node
        para_node += nodes.Text(' required')
        important_node = nodes.important()
        important_node += para_node
        return [important_node]

def setup(app):
    app.add_directive('enterprise-only', EnterpriseOnlyDirective)
