=============================
Stock Exchange data collector
=============================


.. image:: https://img.shields.io/pypi/v/se_collector.svg
        :target: https://pypi.python.org/pypi/se_collector

.. image:: https://img.shields.io/travis/jziem/se_collector.svg
        :target: https://travis-ci.com/jziem/se_collector

.. image:: https://readthedocs.org/projects/se-collector/badge/?version=latest
        :target: https://se-collector.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status




Simple Stock Exchange data collector


* Free software: Apache Software License 2.0
* Documentation: https://se-collector.readthedocs.io.


Features
--------

* download PDF "Kursblätter" (daily trade history) from ls-x via tools.cron_dail_download_jobs.py
* parse these pdf to json into shares and transactions (16.000.000+ transactions in 2020) via tools.pdf_to_json.py
* load json data to database (postgresql) in parallel for extended usage via tools.json_to_db.py


Next steps / TODO
-----------------
* simulate stock prices by interpolation between transactions
* populate simulated stock prices to event queue
* implement (naive) example trade simulator
* consume stock prices by event queue
* plot simulated market data, stock, real buy/sell operatoins and simulation data in realtime
* enable event/data distribution to ML processes for trade simulations

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
