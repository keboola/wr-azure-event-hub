<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\FunctionalTests;

use Keboola\DatadirTests\DatadirTestCase;

class DatadirTest extends DatadirTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        // Remove EntityPath (alias "event hub name") from connection string if preset.
        // It is optional, and we to test if is bad EntityPath specified.
        $connectionString = (string) getenv('CONNECTION_STRING');
        $connectionString = (string) preg_replace('~EntityPath=[^=;]+~', '', $connectionString);
        putenv("CONNECTION_STRING_NORMALIZED={$connectionString}");
        putenv("CONNECTION_STRING_BAD_ENTITY_PATH={$connectionString}EntityPath=test");
    }
}
