# go-irodsclient
Go iRODS Client implemented in pure Golang

## Import package
```go
import (
    "github.com/cyverse/go-irodsclient/fs"
    "github.com/cyverse/go-irodsclient/irods/types"
    "github.com/cyverse/go-irodsclient/irods/util"
)
```

## Account Configuration YAML
```yaml
host:
  hostname: "data.cyverse.org"
  port: 1247
user:
  username: "USERNAME"
  password: "PASSWORD"
  zone: "iplant"
auth_scheme: "native"
```

Loading a YAML file.
```go
yaml, err := ioutil.ReadFile("account.yml")
if err != nil {
    util.LogErrorf("err - %v", err)
    panic(err)
}

account, err := types.CreateIRODSAccountFromYAML(yaml)
if err != nil {
    util.LogErrorf("err - %v", err)
    panic(err)
}
```

## FileSystem Interface
Creating a file system object with default configurations.
```go
appName := "delete_file"
filesystem, err := fs.NewFileSystemWithDefault(account, appName)
if err != nil {
    panic(err)
}
defer filesystem.Release()
```

Deleting a file and double check the file existance.
```go
err = filesystem.RemoveFile("/iplant/home/iychoi/test", true) // do it forcefully
if err != nil {
    util.LogErrorf("err - %v", err)
    panic(err)
}

if !filesystem.ExistsFile("/iplant/home/iychoi/test") {
    fmt.Printf("Successfully deleted file\n")
} else {
    fmt.Printf("Could not delete file\n")
}
```

Downloading a file.
```go
err = filesystem.DownloadFile("/iplant/home/iychoi/test", "/opt")
if err != nil {
    util.LogErrorf("err - %v", err)
    panic(err)
}
```


More examples can be found in `/examples` directory.

## License

Copyright (c) 2010-2021, The Arizona Board of Regents on behalf of The University of Arizona

All rights reserved.

Developed by: CyVerse as a collaboration between participants at BIO5 at The University of Arizona (the primary hosting institution), Cold Spring Harbor Laboratory, The University of Texas at Austin, and individual contributors. Find out more at http://www.cyverse.org/.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

 * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
 * Neither the name of CyVerse, BIO5, The University of Arizona, Cold Spring Harbor Laboratory, The University of Texas at Austin, nor the names of other contributors may be used to endorse or promote products derived from this software without specific prior written permission.


Please check [LICENSE](https://github.com/cyverse/go-irodsclient/tree/master/LICENSE) file.
