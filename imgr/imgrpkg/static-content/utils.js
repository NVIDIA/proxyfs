/*
    Copyright (c) 2015-2021, NVIDIA CORPORATION.
    SPDX-License-Identifier: Apache-2.0
*/

function backToTop() {
    document.body.scrollTop = 0;
    document.documentElement.scrollTop = 0;
}

function getBackToTopButton() {
    return document.getElementById("btn-back-to-top");
}

function defineOnScrollBehavior(back_to_top_button) {
    window.onscroll = function () {
        if (document.body.scrollTop > 20 || document.documentElement.scrollTop > 20) {
            back_to_top_button.style.display = "block";
        } else {
            back_to_top_button.style.display = "none";
        }
    };
}

function addBackToTopBehavior() {
    let back_to_top_button = getBackToTopButton();
    defineOnScrollBehavior(back_to_top_button);
    back_to_top_button.addEventListener("click", backToTop);
}
