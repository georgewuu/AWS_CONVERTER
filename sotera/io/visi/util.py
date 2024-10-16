import math
import logging

logger = logging.getLogger(__name__)

def _overlap(A, B):
    if B[0] < A[0] and B[1] < A[0]:
        return False
    if B[0] > A[1] and B[1] > A[1]:
        return False
    return True

def calculate_blocks(chunkmap, existing_blocks=None):
    if existing_blocks:
        blocks = []
        segments = sorted(set([m['segment'] for m in chunkmap]))
        for seg in segments:
            segment_map =  [ m for m in chunkmap if m['segment'] == seg ]
            segment_existing_blocks = [ b for b in existing_blocks if b['segment'] == seg ]
            for b in segment_existing_blocks:
                sn0 = b['min_sn']
                sn1 = b['max_sn']
                chunks = [ c for c in segment_map if _overlap( c['sn'], (sn0, sn1)) ]
                blocks.append( { 'num': b['num'], 'min_sn': sn0, 'max_sn': sn1, 'chunks': chunks } )
    else:
        blocks = []
        segments = sorted(set([m['segment'] for m in chunkmap]))
        for seg in segments:
            segment_map =  [ m for m in chunkmap if m['segment'] == seg ]
            min_sn = segment_map[0]['sn'][0]
            max_sn = segment_map[-1]['sn'][1]
            segment_length_sn = max_sn - min_sn
            if segment_length_sn > 0:
                segment_length_min = segment_length_sn/500./60.
                seg_blocks = int(round(segment_length_min/120.))
                seg_blocks = 1 if seg_blocks < 1 else seg_blocks
                logger.info('export segment {} to {} blocks'.format(seg,seg_blocks))
                block_length_sn = math.ceil(segment_length_sn/seg_blocks)
                sn0 = min_sn
                len_ = len(blocks)
                for b in range(len_, len_+seg_blocks):
                    sn1 = min(sn0 + block_length_sn - 1, max_sn)
                    chunks = [ c for c in segment_map if _overlap( c['sn'], (sn0, sn1)) ]
                    blocks.append( { 'segment': seg, 'num': b, 'min_sn': sn0, 'max_sn': sn1, 'chunks': chunks } )
                    sn0 += block_length_sn
    return blocks
