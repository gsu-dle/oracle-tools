<?php

declare(strict_types=1);

use GAState\Tools\OCI\OCI;

require __DIR__ . '/../vendor/autoload.php';

$oci = new OCI(
    username: 'ociuser',
    password: 'ocipass',
    connection_string: 'ocisid'
);

// var_dump($oci->fetch("
//   SELECT
//     *
//   FROM
//     SPRIDEN
//   WHERE
//     SPRIDEN_FIRST_NAME = 'Mickey' AND
//     SPRIDEN_LAST_NAME = 'Mouse'
// "));

$start = microtime(true);
$records = $oci->fetchAll("
  select
    *
  from
    ssbsect
  where
    to_char(ssbsect_activity_date, 'yyyymmdd') >= to_char(current_date - 7, 'yyyymmdd')
");
$elapsed = round(1000 * (microtime(true) - $start), 0);
echo "Fetched " . count($records) . " items in {$elapsed}ms\n";
