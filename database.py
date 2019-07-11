# -*- coding: utf-8 -*-
import os
import random

import six
import fire
import mmh3
import tqdm

from util import iterate_data_files

# out_path에 해당하는 경로에 인자로 넣어준 기간내 유저아이디와 본 아티클을 정리해 놓는다
def groupby(from_dtm, to_dtm, tmp_dir, out_path, num_chunks=10): # 2018100100 2019022200 ./tmp/ ./tmp/train
    from_dtm, to_dtm = map(str, [from_dtm, to_dtm]) # function, 리스트를 받아 리스트 인자들을 함수에 넣은 결과를 다시 리스트로 반환한다.
    fouts = {idx: open(os.path.join(tmp_dir, str(idx)), 'w') for idx in range(num_chunks)} # 딕셔너리 형태로 파일의 객체를 저장, 경로는 tmp_dir\idx
    files = sorted([path for path, _ in iterate_data_files(from_dtm, to_dtm)]) # _ 언더 스코어로 두번째 인자값 무시하고 path만 쓴다, 오름차순대로 정렬된 리스트를 갖는다
    # print(files) 리스트 첫번째 인자 : './res/read\\2018100100_2018100101', 리스트 마지막 인자 : './res/read\\2019022123_2019022200'
    # 유저마다 10개의 파일중 하나의 파일에 들어가게 된다 유저아이디 아티클
    for path in tqdm.tqdm(files, desc="files to path", mininterval=1): # tqdm으로 for문의 상태를 알 수 있다 실행시 상태바가 뜬다! 1초마다 상태바가 갱신된다
        for line in open(path): # path의 파일을 열고 한줄마다 실행해준다
            user = line.strip().split()[0] # 줄의 첫번째 단어가 유저
            chunk_index = mmh3.hash(user, 17) % num_chunks # 유저를 해쉬화하고 10으로 나눈값에 따라 다른 임시파일에 정보가 들어가게 된다
            fouts[chunk_index].write(line) # 미리 만들어둔 딕셔너리로 chunk_index에 해당하는 키값의 파일에 해당 라인을 쓴다

    map(lambda x: x.close(), fouts.values()) # 딕셔너리의 value만 갖고 있는 dict_values객체를 받아 즉 파일들을 받고 map으로 인자를 접근해 전부 닫아준다

    # 파일마다 유저아이디에 따라 본 아티클을 통합해준다, 중복된 유저아이디가 없이 파일마다 유저 아이디 이후 본 아티클이 정리된다
    with open(out_path, 'w') as fout: # 파일을 열고 사용한 후에 닫는 것을 보장하는 문법, try: finally: 구조나 file.__enter__(), __exit__()를 따로 구현한 코드를 안써도 된다. 반환값을 fout이 받는다
        for chunk_idx in fouts.keys(): # 키값에 대해서 모두 반복한다
            _groupby = {} # 언더스코어는 private한 변수임을 의미 import했을때 접근하지 못하는정도의 private수준
            chunk_path = os.path.join(tmp_dir, str(chunk_idx)) # 키값을 붙인 새로운 path를 저장
            for line in open(chunk_path): # 임시 path의 한 라인마다 실행해준다
                tkns = line.strip().split() # 토큰별로 나누어준 리스트를 반환
                userid, seen = tkns[0], tkns[1:] # 토큰의 첫번째는 유저아이디이고 그 이후부터는 본 아티클을 얘기한다
                _groupby.setdefault(userid, []).extend(seen) # userid를 키값으로 설정하구 value는 []를 디폴트로 한다. 해당 키값 value에 seen의 리스트 내용을 뒤에 추가해준다
            for userid, seen in six.iteritems(_groupby): # 딕셔너리의 키값마다 반복하고 키값과 value값을 반환한다
                fout.write('%s %s\n' % (userid, ' '.join(seen))) # 최종 out_path경로 파일에 유저아이디와 본 아티클을 스페이스로 구분하여서 한줄마다 쓴다
            os.remove(os.path.join(tmp_dir, str(chunk_idx)))  # 해당 경로의 파일을 삭제, 이유 모를 에러 발생 직접 삭제해주자...

# ./tmp/dev.users 에 정한 인원수만큼의 유저 아이디를 넣어준다, 개발 과정에서 평가할 사용자 리스트를 추출하는 작업
def sample_users(data_path, out_path, num_users): # ./tmp/dev ./tmp/dev.users --num-users=100
    users = [data.strip().split()[0] for data in open(data_path)] # 문자열 양쪽에 있는 공백 지우고 공백 기준으로 나눈 단어리스트의 첫번째 인자를 리턴하여 리스트로 보관
    random.shuffle(users) # 리스트 내용을 랜덤으로 섞어준다
    users = users[:num_users] # 정한 유저수로 리스트를 슬라이싱해주고
    with open(out_path, 'w') as fout: # 파일을 열고 사용한 후에 닫는 것을 보장하는 문법, try: finally: 구조나 file.__enter__(), __exit__()를 따로 구현한 코드를 안써도 된다. 반환값을 fout이 받는다
        fout.write('\n'.join(users)) # 중간에 끝나도 file.__exit__()를 실행해준다, join으로 리스트 객체 사이에 '\n'을 넣어주고 string으로 써준다

if __name__ == '__main__':
    # 실행시 CLI(커맨드 라인 인터페이스)로 만들어준다.
    fire.Fire({'groupby': groupby,
               'sample_users': sample_users})
