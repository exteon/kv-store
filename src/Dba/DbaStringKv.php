<?php

    namespace Exteon\KvStore\Dba;

    use ArrayAccess;
    use ErrorException;
    use Exception;
    use InvalidArgumentException;
    use Iterator;

    /**
     * @implements ArrayAccess<string,string>
     */
    class DbaStringKv implements ArrayAccess, Iterator
    {
        const RETRY_TIMEOUT = 1;

        const DBA_HANDLERS = ['db4'];

        protected static $dbaHandler;

        protected static $isDbaHandlerInit = false;

        /** @var resource */
        protected $handle;

        /** @var string|null */
        protected $currentKey;

        /** @var bool */
        protected $endOfIteration = false;

        /** @var bool */
        protected $isWritable;

        /**
         * @throws ErrorException
         */
        public function __construct(
            string $path,
            string $mode,
            bool $persistent = false,
            int $maxCount = 1,
            int $timeout = self::RETRY_TIMEOUT
        ) {
            $this->initHandle($path, $mode, $persistent, $maxCount, $timeout);
        }

        /**
         * @param string $path
         * @param string $mode
         * @param bool $persistent
         * @param int $maxCount
         * @param int $timeout
         * @throws ErrorException
         */
        protected function initHandle(
            string $path,
            string $mode,
            bool $persistent,
            int $maxCount,
            int $timeout
        ): void {
            switch ($mode[0]) {
                case 'r':
                    break;
                case 'w':
                case 'c':
                    $this->isWritable = true;
                    break;
                default:
                    throw new InvalidArgumentException(
                        'Mode must be \'r\', \'w\' or \'c\''
                    );
            }
            $this->handle = static::tryGetDba(
                $path,
                $mode,
                $persistent,
                1,
                $maxCount,
                $timeout
            );
        }

        /**
         * @param $path
         * @param $mode
         * @param $persistent
         * @param int $count
         * @param int $maxCount
         * @param int $timeout
         * @return mixed
         * @throws ErrorException
         */
        protected static function tryGetDba(
            $path,
            $mode,
            $persistent,
            int $count,
            int $maxCount,
            int $timeout
        ) {
            $f = $persistent ? 'dba_popen' : 'dba_open';
            $handle = $f($path, $mode . 'd', static::getDbaHandler());
            if (!$handle) {
                if ($count >= $maxCount) {
                    throw new Exception('Cannot acquire DBA lock on ' . $path);
                }
                sleep($timeout);
                return static::tryGetDba(
                    $path,
                    $mode,
                    $persistent,
                    $count + 1,
                    $maxCount,
                    $timeout
                );
            } else {
                return $handle;
            }
        }

        /**
         * @throws ErrorException
         */
        protected static function getDbaHandler(): string
        {
            if (!self::$isDbaHandlerInit) {
                $dbaHandlers = dba_handlers();
                foreach (static::DBA_HANDLERS as $h) {
                    if (in_array($h, $dbaHandlers)) {
                        self::$dbaHandler = $h;
                        break;
                    }
                }
                self::$isDbaHandlerInit = true;
                if (!self::$dbaHandler) {
                    throw new ErrorException('No DBA handlers available');
                }
            }
            return self::$dbaHandler;
        }

        /**
         * @throws ErrorException
         */
        public static function addDbaFileExtension(string $path): string
        {
            return $path . '.' . self::getDbaHandler();
        }

        /**
         * @throws ErrorException
         */
        public static function stripDbaFileExtension($path): ?string
        {
            $pathInfo = pathinfo($path);
            if ($pathInfo['extension'] === static::getDbaHandler()) {
                $path = $pathInfo['dirname'] === '.' ? '' : $pathInfo['dirname'];
                $path .= $path ? '/' : '';
                $path .= $pathInfo['filename'];
                return $path;
            }
            return null;
        }

        public function offsetExists($offset): bool
        {
            if (!is_string($offset)) {
                throw new InvalidArgumentException('Only string keys allowed');
            }
            return dba_exists($offset, $this->handle);
        }

        public function offsetSet($offset, $value): void
        {
            if (!is_string($offset)) {
                throw new InvalidArgumentException('Only string keys allowed');
            }
            if (!is_string($value)) {
                throw new InvalidArgumentException(
                    'Only string values allowed'
                );
            }
            dba_replace($offset, $value, $this->handle);
        }

        public function offsetUnset($offset): void
        {
            dba_delete($offset, $this->handle);
        }

        public function __destruct()
        {
            if ($this->isOpen()) {
                $this->close();
            }
        }

        public function isOpen(): bool
        {
            return (bool)$this->handle;
        }

        public function close(): void
        {
            if ($this->isWritable) {
                dba_optimize($this->handle);
            }
            dba_close($this->handle);
            $this->handle = null;
        }

        public function current(): ?string
        {
            if ($this->endOfIteration) {
                return null;
            }
            if ($this->currentKey === null) {
                $this->rewind();
            }
            if ($this->currentKey !== null) {
                return $this->offsetGet($this->currentKey);
            }
            return null;
        }

        public function rewind(): void
        {
            $firstKey = dba_firstkey($this->handle);
            if (!is_string($firstKey)) {
                $this->currentKey = null;
                $this->endOfIteration = true;
            } else {
                $this->currentKey = $firstKey;
                $this->endOfIteration = false;
            }
        }

        public function offsetGet($offset): ?string
        {
            if (!is_string($offset)) {
                throw new InvalidArgumentException('Only string keys allowed');
            }
            $value = dba_fetch($offset, $this->handle);
            if (!is_string($value)) {
                return null;
            }
            return $value;
        }

        public function next(): void
        {
            if ($this->endOfIteration) {
                return;
            }
            if ($this->currentKey === null) {
                $this->rewind();
            } else {
                $nextKey = dba_nextkey($this->handle);
                if (!is_string($this->currentKey)) {
                    $this->currentKey = null;
                    $this->endOfIteration = true;
                } else {
                    $this->currentKey = $nextKey;
                }
            }
        }

        /**
         * @return string|null
         */
        public function key(): ?string
        {
            return $this->currentKey;
        }

        public function valid(): bool
        {
            return (!$this->endOfIteration);
        }
    }