<?php

    namespace Exteon\KvStore\Dba;

    use ArrayAccess;
    use ErrorException;
    use Iterator;

    class DbaKv implements ArrayAccess, Iterator
    {
        /** @var DbaStringKv */
        protected $dbaStringKv;

        /**
         * @throws ErrorException
         */
        public function __construct(
            string $path,
            string $mode,
            bool $persistent = false,
            int $maxCount = 1,
            int $timeout = DbaStringKv::RETRY_TIMEOUT
        ) {
            $this->dbaStringKv = new DbaStringKv(
                $path,
                $mode,
                $persistent,
                $maxCount,
                $timeout
            );
        }

        /**
         * @throws ErrorException
         */
        public static function addDbaFileExtension(string $path): string
        {
            return DbaStringKv::addDbaFileExtension($path);
        }

        function offsetSet($offset, $value): void
        {
            $this->dbaStringKv->offsetSet($offset, serialize($value));
        }

        function offsetGet($offset)
        {
            $value = $this->dbaStringKv->offsetGet($offset);
            if ($value === null) {
                return null;
            }
            return unserialize($value);
        }

        public function offsetExists($offset): bool
        {
            return $this->dbaStringKv->offsetExists($offset);
        }

        public function offsetUnset($offset): void
        {
            $this->dbaStringKv->offsetUnset($offset);
        }

        public function current()
        {
            $value = $this->dbaStringKv->current();
            if ($value === null) {
                return null;
            }
            return unserialize($value);
        }

        public function next(): void
        {
            $this->dbaStringKv->next();
        }

        public function key(): ?string
        {
            return $this->dbaStringKv->key();
        }

        public function valid(): bool
        {
            return $this->dbaStringKv->valid();
        }

        public function rewind(): void
        {
            $this->dbaStringKv->rewind();
        }

        public function close(): void
        {
            $this->dbaStringKv->close();
        }

        public function isOpen(): bool
        {
            return $this->dbaStringKv->isOpen();
        }
    }