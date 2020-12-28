# near-near-map-function-search2


## ライブラリのインストール
$ pip install -r requirements.txt -t source_xxx


## パッケージング&デプロイ コマンド

```
$ find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf
$ cd source_xxx
$ zip -r ../lambda-package.zip *
$ aws lambda update-function-code --function-name near-near-map-search --zip-file fileb://../lambda-package.zip
```
