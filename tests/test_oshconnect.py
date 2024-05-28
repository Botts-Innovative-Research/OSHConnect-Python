
#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/5/28
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================

import pytest

from oshconnect.oshconnect import OSHConnect


class TestOshConnect:

    def test_oshconnect(self):
        app = OSHConnect()
        assert app is not None
