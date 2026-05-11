# Building
In order to build this package you will need:

1. [`mdBook`](https://rust-lang.github.io/mdBook/) for building the help documentation
1. The `fme-packager`-utility from Safe Software for building the .fpkg.

## On windows cmd.exe:
Something like this should work:
```
git clone https://github.com/eea/eea.reportnet3.api.fme.git
cd eea.reportnet3.api.fme\python
git clone https://github.com/andialbrecht/sqlparse
cd sqlparse
git checkout 0.4.3
cd ..
cd ..
copy /y doc\help\src\Reportnet3AttachmentDownloader.md transformers
for /f "usebackq delims==" %i IN (`python -c "from ruamel.yaml import YAML;print(YAML().load(open('package.yml')).get('version'))"`) DO SET MDBOOK_BOOK__TITLE=eea.reportnet [%i]
git log -1 --pretty=format:%H > .commit_hash
mdbook build doc\help -d ..\..\help\pkg-reportnet3
fme-packager pack . 
```

## On Windows PowerShell

If you are running these steps in PowerShell, use the following equivalent commands:

```powershell
git clone https://github.com/eea/eea.reportnet3.api.fme.git
cd eea.reportnet3.api.fme\python
git clone https://github.com/andialbrecht/sqlparse
cd sqlparse
git checkout 0.4.3
cd ..
cd ..
Copy-Item -Path doc\help\src\Reportnet3AttachmentDownloader.md -Destination transformers -Force
$version = python -c "from ruamel.yaml import YAML; print(YAML().load(open('package.yml')).get('version'))"
$env:MDBOOK_BOOK__TITLE = "eea.reportnet [$version]"
[System.IO.File]::WriteAllText('.commit_hash', (git log -1 --pretty=format:%H), [System.Text.Encoding]::ASCII)
mdbook build doc\help -d .\help\pkg-reportnet3
fme-packager pack .
```


