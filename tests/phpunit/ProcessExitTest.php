<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\Tests;

use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Process\Exception\ProcessTimedOutException;
use Symfony\Component\Process\Process;

class ProcessExitTest extends TestCase
{
    private const TIMEOUT_SECONDS = 10.0;

    public function testProcessWithLeakedHandleExits(): void
    {
        // The script has finished its work and written its output, but it still holds a timer that
        // keeps the event loop alive - the shape of the handle the event hub client can leave
        // behind. Without a deterministic exit it would keep running until something kills it,
        // which is what stalled the writer until the platform timeout.
        $process = new Process(['node', __DIR__ . '/fixtures/process-exit/leakedHandle.js']);
        $process->setTimeout(self::TIMEOUT_SECONDS);

        try {
            $process->run();
        } catch (ProcessTimedOutException $e) {
            Assert::fail(sprintf(
                'The process was still running after %.0f seconds, it did not exit on its own.',
                self::TIMEOUT_SECONDS,
            ));
        }

        Assert::assertSame(0, $process->getExitCode(), 'The process did not exit successfully.');

        // Output is written to a pipe, which process.exit() does not flush on its own, so the
        // deterministic exit must not cost us any of it.
        Assert::assertSame(
            ['stdout1', 'stdout2', 'stdout3', 'stdout4'],
            self::outputLines($process->getOutput()),
        );
        Assert::assertSame(['stderr1'], self::outputLines($process->getErrorOutput()));
    }

    /**
     * @return list<string>
     */
    private static function outputLines(string $output): array
    {
        $lines = [];
        foreach (explode("\n", $output) as $line) {
            $line = trim($line);
            if ($line !== '') {
                $lines[] = $line;
            }
        }

        return $lines;
    }
}
