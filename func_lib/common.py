#!/usr/bin/env python3
# -*- coding: utf-8 -*-

def get_float(fn):
    fl = str('%.15f' % fn).rstrip('0').rstrip('.').split('.')
    nl = 0 if len(fl) == 1 else len(fl[1])
    nl = nl - 1 if nl > 1 and len(fl[-1]) > 0 and fl[-1][-1] != '1' else nl
    return nl