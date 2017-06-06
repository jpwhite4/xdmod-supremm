<?php
namespace CCR;

class DB
{
    public static $mockDatabaseImplementation = null;
    private static $realImplLoaded = false;

    public static function factory($sectionName, $autoConnect = true)
    {
        if (DB::$mockDatabaseImplementation !== null) {

            $cls = DB::$mockDatabaseImplementation[0];
            $fcn = DB::$mockDatabaseImplementation[1];

            return $cls->$fcn($sectionName, $autoConnect);
        }

        if ($realImplLoaded === false) {
            // This code is needed to move the original Config class into
            // a different namespace (since this test class is already loaded)
            eval('?>' . str_replace('namespace CCR', 'namespace RealImplementation\CCR', file_get_contents(__DIR__ . '/../../../../../../../xdmod/classes/CCR/DB.php') ));
            Config::$realImplLoaded = true;
        }

        return \RealImplementation\CCR\DB::factory($sectionName, $autoConnect);
    }
}
