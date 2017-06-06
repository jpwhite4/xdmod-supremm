<?php

namespace Xdmod;

/**
 * This class allows tests to be able to override the default configuration
 * class if the test harness defines \Xdmod\Config::$mockConfigSettings then
 * this variable is returned by the config factory method.
 *
 * If the mockConfigSettings is not overridden the the original Config class
 * implementation is used.
 */
class Config
{
    public static $mockConfigSettings = null;
    private static $realConfigLoaded = false;

    /**
     * Factory method.
     */
    public static function factory()
    {
        if (Config::$mockConfigSettings !== null) {
            return Config::$mockConfigSettings;
        }

        if (Config::$realConfigLoaded === false) {
            // This code is needed to move the original Config class into
            // a different namespace (since this test class is already loaded)
            eval('?>' . str_replace('namespace Xdmod', 'namespace RealImplementation\Xdmod', file_get_contents(__DIR__ . '/../../../../../../../xdmod/classes/Xdmod/Config.php') ));
            Config::$realConfigLoaded = true;
        }

        return \RealImplementation\Xdmod\Config::factory();
    }
}
