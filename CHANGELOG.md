### 1.0.2

#### Bugfixes:

* `DbaStringKv::stripDbaExtension()` was returning a `./` prefixed string for simple paths (i.e. `file.ext`)

### 1.0.1

* Use blocking locking mechanism for dba_open