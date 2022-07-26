<?php

declare(strict_types=1);

namespace GAState\Tools\OCI;

use Exception;
use stdClass;

class OCI
{

    /**
     * @param resource|null $r
     * 
     * @return Exception
     */
    private static function error(mixed $r = null): Exception {
        $e = $r === null ? oci_error() : oci_error($r);
        if ($e === false) $e = ['message' => 'Unknown error'];
        return new Exception($e['message']);
    }

    /**
     * @var ?resource $conn
     */
    private $conn = null;

    public function __construct(
        private string $username,
        private string $password,
        private string $connection_string,
        private string $encoding = 'UTF8',
        private int $session_mode = OCI_DEFAULT,
        private int $prefetch_rows = 5000
    ) {
        $this->conn(force: true);
    }

    /**
     * @return resource
     */
    public function conn(bool $force = false): mixed
    {
        if ($force === true || $this->conn === null) {
            if ($this->conn !== null) {
                @oci_close($this->conn);
                $this->conn = null;
            }

            $conn = oci_connect(
                $this->username, 
                $this->password, 
                $this->connection_string,
                $this->encoding, 
                $this->session_mode
            );

            if ($conn === false) throw OCI::error();

            $this->conn = $conn;
        }

        return $this->conn;
    }

    /**
     * @return array<int, int>
     */
    public function foreach(string $query, callable $callback): array
    {
        $processed = $success = 0;

        $stmt = oci_parse($this->conn(), $query);
        if ($stmt === false) throw OCI::error($this->conn());

        oci_set_prefetch($stmt, $this->prefetch_rows);

        if (oci_execute($stmt, OCI_DEFAULT) !== true) throw OCI::error($stmt);

        while($row = oci_fetch_object($stmt)) {
            $processed++;
            $success += $callback($row) === true;
        }
      
        oci_free_statement($stmt);

        return [$processed, $success];
    }

    /**
     * @return bool
     */
    public function exists(string $query): bool
    {
        return $this->fetch($query) !== false;
    }

    /**
     * @return bool
     */
    public function execute(string $query): bool
    {
        $stmt = oci_parse($this->conn(), $query);
        if ($stmt === false) throw OCI::error($this->conn());

        if (!oci_execute($stmt, OCI_DEFAULT)) {
            $error = OCI::error($stmt);
            oci_free_statement($stmt);
            throw $error;
        }

        oci_free_statement($stmt);

        if (!oci_commit($this->conn())) throw OCI::error($this->conn());

        return true;
    }

    /**
     * @return object|false
     */
    public function fetch(string $query): object|false
    {
        $stmt = oci_parse($this->conn(), $query);
        if ($stmt === false) throw OCI::error($this->conn());

        if (!oci_execute($stmt, OCI_DEFAULT)) {
            $error = OCI::error($stmt);
            oci_free_statement($stmt);
            throw $error;
        }

        $row = oci_fetch_object($stmt);

        oci_free_statement($stmt);

        return $row;
    }

    /**
     * @param string|array<string> $queries blkah blah blah
     * @param string|array<string>|null $keyField
     * @param string|null $keyValue
     * 
     * @return array<int|string, object|string>
     */
    public function fetchAll(
        string|array $queries,
        string|array|null $keyField = null,
        ?string $keyValue = null
    ): array {
        if (!is_array($queries)) $queries = [$queries];

        $records = array();
        foreach ($queries as $query) {
            $results = array();

            $stmt = oci_parse($this->conn(), $query);
            if ($stmt === false) throw OCI::error($this->conn());

            oci_set_prefetch($stmt, $this->prefetch_rows);
    
            if (!oci_execute($stmt, OCI_DEFAULT)) {
                $error = OCI::error($stmt);
                oci_free_statement($stmt);
                throw $error;
            }

            oci_fetch_all($stmt, $results, 0, -1, OCI_FETCHSTATEMENT_BY_ROW + OCI_ASSOC);

            oci_free_statement($stmt);

            if ($keyValue !== null && $keyValue !== '') {
                if ($keyField !== null && (is_array($keyField) || $keyField !== '')) {
                    foreach($results as $row) {
                        if (is_array($keyField)) {
                            $keyFieldValue = "";
                            foreach ($keyField as $k) {
                                $keyFieldValue .= $row[$k];
                            }
                        } else {
                            $keyFieldValue = $row[$keyField];
                        }

                        $records[$keyFieldValue] = $row[$keyValue];
                    }
                } else {
                    foreach($results as $row) {
                        $records[] = $row[$keyValue];
                    }
                }
            } else {
                if ($keyField !== null && (is_array($keyField) || $keyField !== '')) {
                    foreach($results as $row) {
                        $record = new stdClass();
                        foreach ($row as $name => $value) {
                            $record->{$name} = $value;
                        }

                        if (is_array($keyField)) {
                            $keyFieldValue = "";
                            foreach ($keyField as $k) {
                                $keyFieldValue .= $record->{$k};
                            }
                        } else {
                            $keyFieldValue = $record->{$keyField};
                        }

                        $records[$keyFieldValue] = $record;
                    }
                } else {
                    foreach($results as $row) {
                        $record = new stdClass();
                        foreach ($row as $name => $value) {
                            $record->{$name} = $value;
                        }
                        $records[] = $record;
                    }
                }
            }
        }

        return $records;
    }
}