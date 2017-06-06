<?php

namespace DataWarehouse\Query\SUPREMM;

class JobMetadataTest extends \PHPUnit_Framework_TestCase
{
    const TEST_ARTIFACT_PATH = "../../vendor/ubccr/xdmod-test-artifacts/xdmod-supremm/summaries/";

    private function getMockSupremmDbIf($resource_id, $local_job_id, $end_time_ts, $datafile, $schemafile)
    {
        $mockSdb = $this->getMockBuilder('\DataWarehouse\Query\SUPREMM\SupremmDbInterface')
            ->disableOriginalConstructor()
            ->disableOriginalClone()
            ->disableArgumentCloning()
            ->getMock();

        $mockCollection = $this->getMockBuilder('\MongoCollection')
            ->disableOriginalConstructor()
            ->disableOriginalClone()
            ->disableArgumentCloning()
            ->getMock();

        $mockMongo = $this->getMockBuilder('\MongoDB')
            ->disableOriginalConstructor()
            ->disableOriginalClone()
            ->disableArgumentCloning()
            ->getMock();

        $mockSchemaCollection = $this->getMockBuilder('\MongoCollection')
            ->disableOriginalConstructor()
            ->disableOriginalClone()
            ->disableArgumentCloning()
            ->getMock();

        $mockMongo->schema = $mockSchemaCollection;

        $mockMongo->method('selectCollection')
            ->willReturn($mockCollection);

        $mockSdb->method('getResourceConfig')
            ->willReturn(array(
                'handle' => $mockMongo,
                'collection' => 'resource_' . $resource_id
            ));

        $mockSdb->method('lookupJob')
            ->willReturn(array(
                'resource_id' => $resource_id,
                'local_job_id' => $local_job_id,
                'end_time_ts' => $end_time_ts,
                'cpu_user' => null,
                'catastrophe' => null,
                'shared' => false
            ));

        if ($datafile !== null) {
            $summary = json_decode(file_get_contents(self::TEST_ARTIFACT_PATH . $datafile), true);

            $mockCollection->method('findOne')
                ->willReturn($summary);
        }

        if ($schemafile !== null) {
            $schema = json_decode(file_get_contents(self::TEST_ARTIFACT_PATH . $schemafile), true);

            $mockSchemaCollection->method('findOne')
                ->willReturn($schema);
        }

        return $mockSdb;
    }

    public function getMockDatabase($section, $auto) {
        return $this->getMockBuilder('CCR\DB\iDatabase')
            ->disableOriginalConstructor()
            ->disableOriginalClone()
            ->disableArgumentCloning()
            ->getMock();
    }

    public function setUp()
    {
        $this->mockUser = $this->getMockBuilder('XDUser')
            ->disableOriginalConstructor()
            ->disableOriginalClone()
            ->disableArgumentCloning()
            ->getMock();

        \Xdmod\Config::$mockConfigSettings = array(
            'datawarehouse' => array(
                'realms' => array(
                    'SUPREMM' => array(
                        'group_bys' => array(),
                        'statistics' => array()
                    )
                )
            ),
            'rawstatisticsconfig' => array(
                'key'=> 'netdrv_isilon_tx_msgs',
                'name'=> 'Parallel filesystem isilon messages transmitted',
                'units'=> 'messages',
                'per'=> 'node',
                'documentation'=> 'number of messages transmitted by network drive i averaged across nodes, i.e. lnet.-.tx_msgs',
                'dtype'=> 'statistic',
                'visibility'=> 'public',
                'group'=> 'Network I/O Statistics'
            ),
            'organization' => array(
                'name' => 'NAME'
            ),
            'hierarchy' => array(
                'top_level_label' => 'TOP_LABEL',
                'top_level_info' => 'TOP_INFO',
                'middle_level_label' => 'MID_LABEL',
                'middle_level_info' => 'MID_INFO',
                'bottom_level_label' => 'BTM_LABEL',
                'bottom_level_info' => 'BTM_INFO'
            )
        );

        \CCR\DB::$mockDatabaseImplementation = array($this, 'getMockDatabase');
    }

    public function testJobMetadata()
    {
        $mockSdb = $this->getMockSupremmDbIf(1, 4454065, 1441406017, '4454065-1441406017.json', 'timeseries-4.json');


        $jobmd = new JobMetadata($mockSdb);

        $result = $jobmd->getJobMetadata($this->mockUser, 4824787);
    }

    public function testJobSummaryPcp()
    {
        $mockSdb = $this->getMockSupremmDbIf(1, 4824787, 1451650405, '4824787-1451650405.json', 'summary-1.0.7.json');

        $jobmd = new JobMetadata($mockSdb);

        $result = $jobmd->getJobSummary($this->mockUser, 4824787);

        $this->assertArrayHasKey('block', $result);
        $this->assertArrayHasKey('sda', $result['block']);
        $this->assertArrayHasKey('write', $result['block']['sda']);
        $this->assertArrayHasKey('avg', $result['block']['sda']['write']);
        $this->assertArrayHasKey('documentation', $result['block']['sda']['write']);
    }

    public function testJobSummaryTaccstats()
    {
        $mockSdb = $this->getMockSupremmDbIf(1, 6289289, 1451921920, '6289289-1451921920.json', 'summary-0.9.34.json');

        $jobmd = new JobMetadata($mockSdb);

        $result = $jobmd->getJobSummary($this->mockUser, 6289289);

        $this->assertArrayHasKey('block', $result);
        $this->assertArrayHasKey('sda', $result['block']);
        $this->assertArrayHasKey('io_ticks', $result['block']['sda']);
        $this->assertArrayHasKey('avg', $result['block']['sda']['io_ticks']);
        $this->assertArrayHasKey('documentation', $result['block']['sda']['io_ticks']);
    }

    public function testGetJobSummaryNoSchema()
    {
        $mockSdb = $this->getMockSupremmDbIf(1, 6289289, 1451921920, '6289289-1451921920.json', null);

        $jobmd = new JobMetadata($mockSdb);

        $result = $jobmd->getJobSummary($this->mockUser, '1');

        $this->assertArrayHasKey('block', $result);
        $this->assertArrayHasKey('sda', $result['block']);
        $this->assertArrayHasKey('io_ticks', $result['block']['sda']);
        $this->assertArrayHasKey('avg', $result['block']['sda']['io_ticks']);
    }

    public function testJobTimeseriesMetaData()
    {
        $mockSdb = $this->getMockSupremmDbIf(1, 4454065, 1441406017, '4454065-1441406017.json', 'timeseries-4.json');

        $jobmd = new JobMetadata($mockSdb);
        
        $result = $jobmd->getJobTimeseriesMetaData($this->mockUser, 4454065);

        $expected = array(
            array(
                "tsid" => "cpuuser",
                "text" => "CPU User",
                "leaf" => false
            )
        );

        $this->assertEquals($expected, $result);
    }

    public function testJobTimeseriesMetaDataV3()
    {
        $mockSdb = $this->getMockSupremmDbIf(1, 1000000, 1371867036, '1000000-1371867036.json', 'timeseries-3.json');

        $jobmd = new JobMetadata($mockSdb);
        
        $result = $jobmd->getJobTimeseriesMetaData($this->mockUser, 4454065);

        $expected = array (
            array (
                'tsid' => 'lnet',
                'text' => 'Parallel Filesystem traffic',
                'leaf' => false,
            ),
            array (
                'tsid' => 'cpuuser',
                'text' => 'CPU User',
                'leaf' => false,
            ),
            array (
                'tsid' => 'ib_lnet',
                'text' => 'Interconnect MPI traffic',
                'leaf' => false,
            ),
            array (
                'tsid' => 'memused_minus_diskcache',
                'text' => 'Memory usage',
                'leaf' => false,
            )
        );

        $this->assertEquals($expected, $result);
    }
    

    public function testJobTimeseriesMetricMetaData()
    {
        $mockSdb = $this->getMockSupremmDbIf(1, 4454065, 1441406017, '4454065-1441406017.json', 'timeseries-4.json');

        $jobmd = new JobMetadata($mockSdb);
        
        $result = $jobmd->getJobTimeseriesMetricMeta($this->mockUser, 4454065, 'cpuuser');

        $expected = array(
            array(
                'text' => 'cpn-d09-25-02.cbls.ccr.buffalo.edu',
                'leaf' => false,
                'nodeid' => 0
            )
        );

        $this->assertEquals($expected, $result);
    }

    public function testJobTimeseriesMetricMetaDataV3()
    {
        $mockSdb = $this->getMockSupremmDbIf(1, 1000000, 1371867036, '1000000-1371867036.json', 'timeseries-3.json');

        $jobmd = new JobMetadata($mockSdb);
        
        $result = $jobmd->getJobTimeseriesMetricMeta($this->mockUser, 1000000, 'cpuuser');

        $expected = array (
            array (
                'text' => 'c533-601',
                'leaf' => false,
                'nodeid' => 0
            )
        );

        $this->assertEquals($expected, $result);
    }

    public function testJobMetadataClasssNoData()
    {
        $mockSdb = $this->getMockBuilder('\DataWarehouse\Query\SUPREMM\SupremmDbInterface')
            ->disableOriginalConstructor()
            ->disableOriginalClone()
            ->disableArgumentCloning()
            ->getMock();

        $mockSdb->method('lookupJob')
            ->willReturn(null);

        $jobmd = new JobMetadata($mockSdb);

        $result = $jobmd->getJobMetadata($this->mockUser, '1');
        $this->assertEquals(array(), $result);

        $result = $jobmd->getJobSummary($this->mockUser, '1');
        $this->assertEquals(array(), $result);

        $result = $jobmd->getJobExecutableInfo($this->mockUser, '1');
        $this->assertEquals(null, $result);

        $result = $jobmd->getJobTimeseriesData($this->mockUser, '1', 1, 'cpuuser', 1);
        $this->assertEquals(array(), $result);

        $result = $jobmd->getJobTimeseriesMetaData($this->mockUser, '1');
        $this->assertEquals(array(), $result);

        $result = $jobmd->getJobTimeseriesMetricMeta($this->mockUser, '1', 'cpuuser');
        $this->assertEquals(array(), $result);

        $result = $jobmd->getJobTimeseriesMetricNodeMeta($this->mockUser, '1', 'cpuuser', 1);
        $this->assertEquals(array(), $result);
        
    }

    public function testJobMetadataClasssNoMongo()
    {
        $mockSdb = $this->getMockBuilder('\DataWarehouse\Query\SUPREMM\SupremmDbInterface')
            ->disableOriginalConstructor()
            ->disableOriginalClone()
            ->disableArgumentCloning()
            ->getMock();

        $mockSdb->method('lookupJob')
            ->willReturn(array(
                'resource_id' => 1,
                'local_job_id' => 12231243,
                'end_time_ts' => 12348894
            ));

        $jobmd = new JobMetadata($mockSdb);

        $result = $jobmd->getJobSummary($this->mockUser, '1');
        $this->assertEquals(null, $result);

        $result = $jobmd->getJobExecutableInfo($this->mockUser, '1');
        $this->assertEquals(array(), $result);

        $result = $jobmd->getJobTimeseriesData($this->mockUser, '1', 1, 'cpuuser', 1);
        $this->assertEquals(array(), $result);

        $result = $jobmd->getJobTimeseriesMetaData($this->mockUser, '1');
        $this->assertEquals(array(), $result);

        $result = $jobmd->getJobTimeseriesMetricMeta($this->mockUser, '1', 'cpuuser');
        $this->assertEquals(array(), $result);

        $result = $jobmd->getJobTimeseriesMetricNodeMeta($this->mockUser, '1', 'cpuuser', 1);
        $this->assertEquals(array(), $result);
        
    }
    
    public function testJobExecutableInfoPcp()
    {
        $mockSdb = $this->getMockSupremmDbIf(1, 4824787, 1451650405, '4824787-1451650405.json', 'summary-1.0.7.json');
        
        $jobmd = new JobMetadata($mockSdb);

        $result = $jobmd->getJobExecutableInfo($this->mockUser, 4824787);

        $this->assertArrayHasKey('constrained', $result);
        $this->assertArrayHasKey('unconstrained', $result);
        $this->assertArrayHasKey('cpusallowed', $result);
    }
    
    public function testJobExecutableInfoLariat()
    {
        $mockSdb = $this->getMockSupremmDbIf(1, 6289289, 1451921920, '6289289-1451921920.json', 'summary-0.9.34.json');
        
        $jobmd = new JobMetadata($mockSdb);

        $result = $jobmd->getJobExecutableInfo($this->mockUser, 6289289);

        $this->assertArrayHasKey('user', $result);
        $this->assertEquals('username', $result['user']);
    }

    public function testJobExecutableInfoRedactedLariat()
    {
        $mockSdb = $this->getMockSupremmDbIf(1, 6289289, 1451921920, '6289289-1451921920.json', 'summary-0.9.34.json');

        $jobmd = new JobMetadata($mockSdb);

        $this->mockUser->method('getUserType')
            ->willReturn(DEMO_USER_TYPE);

        $result = $jobmd->getJobExecutableInfo($this->mockUser, 6289289);

        $this->assertEquals(htmlentities('<REDACTED>'), $result['user']);
        $this->assertEquals(htmlentities('<REDACTED>'), $result['cwd']);
        $this->assertEquals(htmlentities('<REDACTED>'), $result['fn']);
    }

    /**
     * @dataProvider arrayMergeTestdata
     */
    public function testArrayMerge($left, $right, $expected)
    {
        $jobmd = new \TestHelpers\mock\JobMetadataWorkaround();

        $arrayMergeFn = \TestHelpers\TestHelper::unlockMethod($jobmd, 'arrayMergeRecursiveWildcard');
        $result = $arrayMergeFn->invoke($jobmd, $left, $right);

        $this->assertEquals($expected, $result);
    }

    public function arrayMergeTestdata()
    {
        $output = array();

        $left = array(
            "one" => array("data" => 1),
            "two" => array("data" => 2),
            "three" => array("data" => 3)
        );

        $right = array(
            "one" => array("doc" => 1),
            "two" => array("doc" => 2),
            "three" => array("doc" => 3)
        );

        $expected = array(
            "one" => array("data" => 1, "doc" => 1),
            "two" => array("data" => 2, "doc" => 2),
            "three" => array("data" => 3, "doc" => 3)
        );

        $output[] = array($left, $right, $expected);

        $left = array(
            "metric" => array(
                "device1" => array("data" => 1),
                "device2" => array("data" => 2)
            )
        );

        $right = array(
            "metric" => array(
                "*" => array("doc" => 1)
            )
        );

        $expected = array(
            "metric" => array(
                "device1" => array("data" => 1, "doc" => 1),
                "device2" => array("data" => 2, "doc" => 1)
            )
        );

        $output[] = array($left, $right, $expected);

        $left = json_decode(file_get_contents(self::TEST_ARTIFACT_PATH . '4824787-1451650405-gpu.json'), true);

        $schema = json_decode(file_get_contents(self::TEST_ARTIFACT_PATH . 'summary-1.0.7-gpu.json'), true);
        $right = $schema['definitions'];
        $expected = $left;

        $output[] = array($left, $right, $expected);

        return $output;

    }
}
