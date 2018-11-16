#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import twint

c = twint.Config()
c.Search = "문재인"
c.Lang = "ko"
c.Since = "2017-05-10"
c.Until = "2018-11-03"
c.Timedelta = 1
c.Store_csv = True
c.Output = "data"

twint.run.Search(c)