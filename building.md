# Building
In order to build this package you will need:

1. [`mdBook`](https://rust-lang.github.io/mdBook/) for building the help documentation
1. The `fpkgr`-utility from Safe Software for building the .fpkg.

On windows, something like following should work:

```
git clone https://github.com/eea/eea.reportnet3.api.fme.git
cd eea.reportnet3.api.fme\python
git clone https://github.com/andialbrecht/sqlparse
cd ..
copy /y doc\help\src\Reportnet3AttachmentDownloader.md transformers
for /f "usebackq delims==" %i IN (`python -c "from ruamel.yaml import YAML;print(YAML().load(open('package.yml')).get('version'))"`) DO SET MDBOOK_BOOK__TITLE=eea.reportnet [%i]
mdbook build doc\help -d ..\..\help\pkg-reportnet3
fpkgr pack . 
```


