<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\MessageMapper;

use Keboola\Csv\CsvReader;
use Keboola\AzureEventHubWriter\Configuration\Config;
use Keboola\AzureEventHubWriter\Configuration\ConfigDefinition;
use Keboola\AzureEventHubWriter\Exception\UnexpectedValueException;

class MessageMapperFactory
{
    private Config $config;

    public function __construct(Config $config)
    {
        $this->config = $config;
    }

    public function create(CsvReader $csvReader): MessageMapper
    {
        switch ($this->config->getMode()) {
            case ConfigDefinition::MODE_MESSAGE_ROW_AS_JSON:
                return new RowAsJsonMapper($csvReader);
            case ConfigDefinition::MODE_MESSAGE_COLUMN_VALUE:
                return new ColumnValueMapper($this->config, $csvReader);
            default:
                throw new UnexpectedValueException(sprintf('Unexpected mode "%s".', $this->config->getMode()));
        }
    }
}
