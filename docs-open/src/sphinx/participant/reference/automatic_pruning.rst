..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. |br| raw:: html

   <br />

.. _ledger-pruning-automatic:

Automatic Pruning
-----------------

The following functions are available to set, modify, and read the pruning schedule:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/pruning/PruningDocumentationTest.scala
   :start-after: user-manual-entry-begin: AutoPruneAllMethods
   :end-before: user-manual-entry-end: AutoPruneAllMethods
   :dedent:

Refer to the cron specification to customize the pruning schedule. Here are a few examples:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/pruning/PruningDocumentationTest.scala
   :start-after: user-manual-entry-begin: PruningScheduleExamples
   :end-before: user-manual-entry-end: PruningScheduleExamples
   :dedent:

For the maximum duration to specify a reliable pruning window end time, the leading fields of the cron expression
must not be wildcards (`*`), as illustrated in the preceding examples. If the hour field is fixed, the
fields for the minute and the second must be fixed too.

Schedule format
***************

Cron expressions are formatted as seven whitespace-separated fields:

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Field
     - Type and valid values\*
   * - seconds
     - number from ``0`` to ``59``
   * - minutes
     - number from ``0`` to ``59``
   * - hours
     - number from ``0`` to ``23``
   * - day of the month
     - number from ``1`` to ``31``
   * - month
     - number from ``0`` to ``11`` (``0`` = January)
       |br|
       **or**
       |br|
       the first three letters of the month name (``JAN``, ``FEB``, ``MAR``, etc)
   * - day of the week
     - number from ``1`` to ``7`` (``1`` = Sunday)
       |br|
       **or**
       |br|
       the first three letters of the day name (``SUN``, ``MON``, ``TUE``, etc)
   * - year (optional)
     - number from ``1900`` to ``2099``

*\*Ranges specified with "from .. to .." are inclusive of both endpoints.*

Note that, although a day-of-the-month value might be valid according to the
preceding definition, it might not correspond to an actual date in certain months
(such as the thirty-first of November). If you schedule pruning for the thirty-first
of the month, every month with fewer than 31 days is skipped.

Advanced schedule formatting
****************************

* You can construct **lists and ranges** of values. For example, the day of the
  week could be a range like ``MON-FRI`` to refer to the days Monday through
  Friday, or ``TUE,FRI`` to refer to Tuesday and Friday exclusively. Or you
  could use a mix of both, for example, ``MON,WED-FRI``, meaning "Monday, and
  also Wednesday through Friday."
* Use the **asterisk** (``*``) as a wildcard that means "all possible values."
* Use the **question mark** (``?``) as a wildcard that means "any value" in the
  day-of-the-month and day-of-the-week fields. For example, to specify "every
  Monday at noon," use the ``?`` character to indicate that any day of the
  month is valid: ``0 0 12 ? * MON``
* To apply **increments** to numeric values, use the slash character (``/``).
  For example, a value of ``1/2`` in the hours field means "every two hours
  starting from 1 AM" (1 AM, 3 AM, 5 AM, etc).

Here are some examples of valid schedules:

* ``0 30 * * * *`` Every hour at half past
* ``0 5/15 12,18-20 * * *`` Every fifteen minutes, starting from five past,
  at noon and from 6 to 8 PM
* ``0 5/15 12,18-20 ? * MON,THU`` Same as above, but only on Mondays and
  Thursdays
* ``0 0 22 1 * ?`` Every first day of the month at 10 PM

For more information about cron expressions, see the `Apache Log4j API
documentation
<https://logging.apache.org/log4j/2.x/javadoc/log4j-core/org/apache/logging/log4j/core/util/CronExpression.html>`_.
