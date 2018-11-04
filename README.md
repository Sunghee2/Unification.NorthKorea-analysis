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

