# President-Moon-Jae-in

### Todo

**2018-11-04**

- [x] tweet scraper 찾기 (twint, twitter-scraper)
- [x] 데이터 수집..

> :memo:
>
> 트위터 api 이용하는 것은(tweepy) 7일 이내 데이터만 가능하고 이전 자료를 보려면 돈을 내야됨 -> 웹에서 긁어모으자...
>
> 라이브러리 twint, twitter-scraper.. firefox 무한스크롤 이용해서 직접……
>
> twitter-scraper는 25페이지 정도까지만 보장 가능(486트윗) -> twint 사용
>
> python2.* => `$ python` python3.* => `$ py`
>
> :bug:
>
> `Command "/Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7 -u -c "import setuptools, tokenize;__file__='/private/tmp/pip-install-pdut0psv/cchardet/setup.py';`..-> twint install이 안됨.  이것저것 하다가 python 요구버전이 3.6이라 윈도우로 옮겨서 3.6.7깔았더니 해결...
>
> 실행했더니 => `ModuleNotFoundError: No module named 'aiohttp_socks'` -> twint uninstall하고 `pip3 install --upgrade -e git+https://github.com/twintproject/twint.git@origin/master#egg=twint`                    이렇게 설치했더니 해결
>
> <br/>
>
> 이렇게 쉽게 해도 되는 걸까….ㅠ… 바로 디비에 저장해야되나..? 



##### 2018-11-06

- [x] twint 에러 해결
- [x] vscode 연결
- [ ] 데이터 파일 read 
- [ ] nifi 설치 —> 했는데 hortonworks로 다시 깔기 

> :memo:
>
> twint 정지계정트윗나면 에러남 -> output에 에러처리해주기(나중에 올려줘야지..)
>
> <br/>
>
> vm과 vscode 연결
>
> 1. vscode에서 extentsions 'Remote VSCode' 설치
>
> 2. rmate 설치
>
>    ```
>    wget https://raw.githubusercontent.com/sclukey/rmate-python/master/bin/rmate
>    chmod +x ./rmate
>    sudo mv ./rmate /usr/local/bin/rmate 
>    ```
>
> 3. `$ ssh -R 52698:localhost:52698 maria_dev@localhost -p 2222`
>
> 4. `$ rmate project/tw.py`
>
> :bug:
>
> hdfs에 test파일 올렸는데 한글 다 깨짐 -> `$ echo $LANG` `$ locale` 보면 제대로(ko_KR.UTF-8) 되어있는데ㅠ
>
> df로 만들면 스키마가 이상하게 c1, c2… 이렇게 됨…..  ---> csv load하면서 `header="true"` 빼먹음
>
> `UnicodeEncodeError: 'ascii' codec can't encode characters in position 1551-1552: ordinal not in range(128)` 파일 불러올 때 인코딩 설정하는데도 왜이러지...





- [ ] timezone.. 같은 열 삭제
- [ ] 같은 date, time, id인 행 삭제
- [ ] hdfs로 보내기
- [ ] hdfs파일 
- [ ] git--sandbox