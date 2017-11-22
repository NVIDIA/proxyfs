====================
ProxyFS Architecture
====================

Overview
========

.. toctree::
    :maxdepth: 1

    ldlm_requirements
    bimodal-support

uml
===

The `uml` directory contains .uml files that are used as input to
PlantUML (http://plantuml.com). PlantUML produces .png files which
can then be displayed. The .png files illustrate the architecture
of certain operations such as a bimodal PUT operation.

Then do something like:

    # java -jar plantuml.jar read.uml

This will produce a correspondingly named .png file you can display.
