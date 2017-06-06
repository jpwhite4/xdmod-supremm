<?php

namespace TestHelpers\mock;

class MockSupremmDbInterface
{
    public function __construct($mockmongo)
    {
        $this->mockmongo = $mockmongo;
    }

    public function getResourceConfig($resource_id)
    {
        return array(
            'handle' => $this->mockmongo,
            'collection' => 'test',

    }

    public function getsummaryschema($resource_id, $summary_version)
    {
    }

    public function getdbstats($resource_id)
    {
    }
}
