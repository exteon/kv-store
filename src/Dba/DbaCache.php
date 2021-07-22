<?php

    namespace Exteon\KvStore\Dba;

    use ErrorException;
    use Exteon\Patterns\Cache\AbstractCache;

    class DbaCache extends AbstractCache
    {
        /** @var string */
        protected $path;

        /**  @var int */
        protected $maxCount;

        /** @var int */
        protected $timeout;

        /** @var DbaKv|null */
        protected $dba;

        /** @var bool */
        protected $isSetupRead;

        /** @var bool */
        protected $isSetupWrite;

        public function __construct(
            string $path,
            int $maxCount = 1,
            int $timeout = DbaStringKv::RETRY_TIMEOUT
        ) {
            $this->path = $path;
            $this->maxCount = $maxCount;
            $this->timeout = $timeout;
        }

        public function purge(): void
        {
            if (file_exists($this->path)) {
                unlink($this->path);
            }
        }

        protected function getKvStore()
        {
            return $this->dba;
        }

        /**
         * @throws ErrorException
         */
        protected function setupRead(): void
        {
            if (
                $this->isSetupRead ||
                $this->isSetupWrite
            ) {
                return;
            }
            if(file_exists($this->path)){
                $this->dba = new DbaKv(
                    $this->path,
                    'r',
                    true,
                    $this->maxCount,
                    $this->timeout
                );
            }
            $this->isSetupRead = true;
        }

        /**
         * @throws ErrorException
         */
        protected function setupWrite(): void
        {
            if ($this->isSetupWrite) {
                return;
            }
            if ($this->dba) {
                $this->dba->close();
            }
            $this->dba = new DbaKv(
                $this->path,
                'c',
                false,
                $this->maxCount,
                $this->timeout
            );
            $this->isSetupWrite = true;
        }

        public function exists(): bool
        {
            return file_exists($this->path);
        }
    }