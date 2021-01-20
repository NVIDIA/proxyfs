# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0


class MetaMiddleware(object):
    def __init__(self, app, conf):
        self.app = app

    def __call__(self, env, start_response):
        hToDel = list()
        vToAdd = list()
        for h in env:
            if h.upper() == 'HTTP_X_PROXYFS_BIMODAL':
                hToDel.append(h)
                vToAdd.append(env[h])
        for h in hToDel:
            del env[h]
        for v in vToAdd:
            # NB: only last one, if multiple, will determine value
            env['HTTP_X_ACCOUNT_SYSMETA_PROXYFS_BIMODAL'] = v

        def meta_response(status, response_headers, exc_info=None):
            hvToDel = list()
            vToAdd = list()
            for (h, v) in response_headers:
                if h.upper() == 'X-ACCOUNT-SYSMETA-PROXYFS-BIMODAL':
                    hvToDel.append((h, v))
                    vToAdd.append(v)
            for hv in hvToDel:
                response_headers.remove(hv)
            for v in vToAdd:
                # potentially multiple instances of same header
                response_headers.append(('X-ProxyFS-BiModal', v))
            return start_response(status, response_headers, exc_info)

        return self.app(env, meta_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def meta_filter(app):
        return MetaMiddleware(app, conf)

    return meta_filter
