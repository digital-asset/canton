# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from pygments.lexers.haskell import HaskellLexer
from pygments.lexer import inherit
from pygments.token import *

# Import the Daml lexer
def setup(sphinx):
    from pygments_daml_lexer import DAMLLexer
    sphinx.add_lexer("daml", DAMLLexer)

class DAMLLexer(HaskellLexer):

    name = 'Daml'
    aliases = ['daml']
    filenames = ['*.daml']

    daml_reserved = ('template', 'with', 'controller', 'can', 'ensure', 'daml', 'observer', 'signatory', 'controller', 'nonconsuming', 'return', 'this')

    tokens = {
        'root': [
            (r'\b(%s)(?!\')\b' % '|'.join(daml_reserved), Keyword.Reserved),
            (r'\b(True|False)\b', Keyword.Constant),
            inherit
        ]
    }



