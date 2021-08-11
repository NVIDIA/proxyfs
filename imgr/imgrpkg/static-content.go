// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

//go:generate ../../make-static-content/make-static-content imgrpkg stylesDotCSS              text/css                      s static-content/styles.css                                         styles_dot_css_.go

//go:generate ../../make-static-content/make-static-content imgrpkg jsontreeDotJS             application/javascript        s static-content/jsontree.js                                        jsontree_dot_js_.go

//go:generate ../../make-static-content/make-static-content imgrpkg bootstrapDotCSS           text/css                      s static-content/bootstrap.min.css                                  bootstrap_dot_min_dot_css_.go

//go:generate ../../make-static-content/make-static-content imgrpkg bootstrapDotJS            application/javascript        s static-content/bootstrap.min.js                                   bootstrap_dot_min_dot_js_.go
//go:generate ../../make-static-content/make-static-content imgrpkg jqueryDotJS               application/javascript        s static-content/jquery.min.js                                      jquery_dot_min_dot_js_.go
//go:generate ../../make-static-content/make-static-content imgrpkg popperDotJS               application/javascript        b static-content/popper.min.js                                      popper_dot_min_dot_js_.go

//go:generate ../../make-static-content/make-static-content imgrpkg openIconicBootstrapDotCSS text/css                      s static-content/open-iconic/font/css/open-iconic-bootstrap.min.css open_iconic_bootstrap_dot_css_.go

//go:generate ../../make-static-content/make-static-content imgrpkg openIconicDotEOT          application/vnd.ms-fontobject b static-content/open-iconic/font/fonts/open-iconic.eot             open_iconic_dot_eot_.go
//go:generate ../../make-static-content/make-static-content imgrpkg openIconicDotOTF          application/font-sfnt         b static-content/open-iconic/font/fonts/open-iconic.otf             open_iconic_dot_otf_.go
//go:generate ../../make-static-content/make-static-content imgrpkg openIconicDotSVG          image/svg+xml                 s static-content/open-iconic/font/fonts/open-iconic.svg             open_iconic_dot_svg_.go
//go:generate ../../make-static-content/make-static-content imgrpkg openIconicDotTTF          application/font-sfnt         b static-content/open-iconic/font/fonts/open-iconic.ttf             open_iconic_dot_ttf_.go
//go:generate ../../make-static-content/make-static-content imgrpkg openIconicDotWOFF         application/font-woff         b static-content/open-iconic/font/fonts/open-iconic.woff            open_iconic_dot_woff_.go
