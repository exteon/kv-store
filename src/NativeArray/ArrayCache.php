<?php
    namespace Exteon\KvStore\NativeArray;

    use ArrayObject;
    use Exteon\Patterns\Cache\AbstractCache;

    class ArrayCache extends AbstractCache
    {
        /** @var ArrayObject|null */
        private $arrayStore = null;

        protected function getKvStore(): ?ArrayObject
        {
            return $this->arrayStore;
        }

        protected function setupRead(): void
        {
        }

        protected function setupWrite(): void
        {
            if($this->arrayStore === null){
                $this->arrayStore = new ArrayObject();
            }
        }

        public function exists(): bool
        {
            return ($this->arrayStore !== null);
        }

        public function purge(): void
        {
            $this->arrayStore = null;
        }
    }