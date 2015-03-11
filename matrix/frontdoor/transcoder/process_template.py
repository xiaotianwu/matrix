__author__ = 'yijiliu@chinacache.com'

import json
#import find_template
import re

mod_root = "/matrix/matrix/frontdoor/transcoder/"

def find_template(vcodec_value, acodec_value, bit_value, res_value):
    str1 = mod_root + 'mod_%s_%s_%s_%s'%(vcodec_value, acodec_value, bit_value, res_value)
    str2 = mod_root + 'mod_%s_%s_%s'%(vcodec_value, acodec_value, bit_value)
    str3 = mod_root + 'mod_%s_%s'%(vcodec_value, acodec_value)

    f_in = open(mod_root + 'mod_template.json','r')
    s = json.load(f_in)

    for str0 in [str1, str2, str3]:
        for line in s:
            m = re.search(str0, s[line])
            if m != None:
                break
        if m != None:
            break
    return  s[line]

def process_template(input_path, output_path, bit_value, res_value, vcodec_value, acodec_value):
    file_name = find_template(vcodec_value, acodec_value, bit_value, res_value)

    f_in = open(file_name,'r')
    s = json.load(f_in)

    for line in s:
        if line == '-f':
            contain_format = s[line]
        elif line == '-preset':
            preset = s[line]
        elif line == '-profile:v':
            profile = s[line]
        elif line == 'level:v':
            level = s[line]
        elif line == '-x264opts':
            keyframe = s[line]

    ffmpeg_cmd = 'ffmpeg -i %s -preset %s -profile:v %s level:v %s -x264opts %s -b %sk -s %s -vcodec %s -acodec %s -f %s -y %s'%(input_path, preset, profile, level, keyframe, bit_value, res_value, vcodec_value, acodec_value, contain_format, output_path)
    return ffmpeg_cmd

#process_template('rtmp://lxrtmp.load.cdn.zhanqi.tv/zqlive/27372_DdopD','rtmp://lxrtmp.load.cdn.zhanqi.tv/zqlive/newstreams', '500', '704*572', 'h264', 'aac')
