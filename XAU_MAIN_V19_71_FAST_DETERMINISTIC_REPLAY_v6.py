import math, os, json, time
from pathlib import Path
import pandas as pd
import numpy as np
from numba import njit

M1_PATH=Path('/mnt/data/m1_2024/XAUUSD_M1_2024_BID_ASK.csv')
WARM_PATH=Path('/mnt/data/m1_warmup/XAUUSD_M1_BID_ASK_2010.csv')
OUT_PATH=Path('/mnt/data/XAU_MAIN_V19_71_REPLAY_2024_v6.csv')
DEC_PATH=Path('/mnt/data/XAU_MAIN_V19_71_REPLAY_2024_DECISIONS_v6.csv')
REPORT_PATH=Path('/mnt/data/XAU_MAIN_V19_71_REPLAY_2024_REPORT_v6.json')
MIN_SCORE=50
A=1.0/14.0

@njit(cache=False)
def frame_calc(o,h,l,c,v):
    n=len(c)
    ema50=ema200=macd=macd_sig=atr=0.0
    ag=al=0.0; plus_e=minus_e=0.0; adx=0.0
    # Initialize like adjust=False EWMs from first observation.
    ema50=o[0]*0+c[0]; ema200=c[0]
    fast=c[0]; slow=c[0]; macd=0.0; macd_sig=0.0
    prev=c[0]
    prev_h=h[0]; prev_l=l[0]
    plus_dm_e=minus_dm_e=0.0; dx_ewm=0.0
    for i in range(n):
        ci=c[i]
        if i==0:
            tr=h[i]-l[i]; gain=0.0; loss=0.0; up=0.0; down=0.0
        else:
            tr=max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1]))
            d=ci-c[i-1]; gain=d if d>0 else 0.0; loss=-d if d<0 else 0.0
            up=h[i]-h[i-1]; down=-(l[i]-l[i-1])
            plus_dm=up if (up>down and up>0) else 0.0
            minus_dm=down if (down>up and down>0) else 0.0
            # ewm after first observation
            plus_dm_e=A*plus_dm+(1-A)*plus_dm_e
            minus_dm_e=A*minus_dm+(1-A)*minus_dm_e
        if i>0:
            ema50=(2.0/51.0)*ci+(1-2.0/51.0)*ema50
            ema200=(2.0/201.0)*ci+(1-2.0/201.0)*ema200
            fast=(2.0/9.0)*ci+(1-2.0/9.0)*fast
            slow=(2.0/22.0)*ci+(1-2.0/22.0)*slow
            macd=fast-slow
            macd_sig=(2.0/6.0)*macd+(1-2.0/6.0)*macd_sig
            ag=A*gain+(1-A)*ag
            al=A*loss+(1-A)*al
            atr=A*tr+(1-A)*atr
        else:
            atr=tr; ag=0.0; al=0.0; plus_dm_e=minus_dm_e=0.0
        # ADX after +DI/-DI update; first bar uses zeros.
        plus_di=100.0*plus_dm_e/atr if atr!=0 else np.nan
        minus_di=100.0*minus_dm_e/atr if atr!=0 else np.nan
        den=plus_di+minus_di
        dx=abs(plus_di-minus_di)*100.0/den if np.isfinite(den) and den!=0 else 0.0
        if i==0: dx_ewm=0.0
        else: dx_ewm=A*dx+(1-A)*dx_ewm
        adx=dx_ewm
    # final RSI
    if al==0 and ag>0: rsi=100.0
    elif ag==0 and al>0: rsi=0.0
    elif ag==0 and al==0: rsi=50.0
    else: rsi=100.0-100.0/(1.0+ag/al)
    # structure and volume based on final window
    struct=0
    if n>=5:
        if h[-1]>h[-5] and l[-1]>l[-5]: struct=1
        elif h[-1]<h[-5] and l[-1]<l[-5]: struct=-1
    start=max(0,n-20); vm=0.0
    for j in range(start,n): vm+=v[j]
    vm/=max(1,n-start)
    vr=v[-1]/vm if vm>0 else 0.0
    bull=bear=0.0
    price=c[-1]
    if price>ema50: bull+=10
    elif price<ema50: bear+=10
    if price>ema200: bull+=15
    elif price<ema200: bear+=15
    if rsi>=60: bull+=10 if rsi>=65 else 7
    elif rsi>=55: bull+=5
    elif rsi<=40: bear+=10 if rsi<=35 else 7
    elif rsi<=45: bear+=5
    mh=macd-macd_sig
    if macd>macd_sig:
        bull+=10
        if mh>0: bull+=5
    elif macd<macd_sig:
        bear+=10
        if mh<0: bear+=5
    ap=10 if adx>=25 else 6 if adx>=18 else 2
    if plus_di>minus_di: bull+=ap
    elif minus_di>plus_di: bear+=ap
    if struct==1: bull+=15
    elif struct==-1: bear+=15
    vp=10 if vr>=1.40 else 8 if vr>=1.20 else 5 if vr>=1.00 else 2 if vr>=0.90 else 0
    if vp and n>=2:
        if price>o[-1] and price>=c[-2]: bull+=vp
        elif price<o[-1] and price<=c[-2]: bear+=vp
    direction=1 if bull>bear else -1 if bear>bull else 0
    score=int(min(100,abs(bull-bear)))
    return price,ema50,ema200,rsi,macd,macd_sig,mh,adx,plus_di,minus_di,atr,struct,vr,bull,bear,score,direction

@njit(cache=False)
def ewma_atr_last(h,l,c):
    n=len(c); atr=0.0
    for i in range(n):
        if i==0: tr=h[i]-l[i]
        else: tr=max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1]))
        if i==0: atr=tr
        else: atr=A*tr+(1-A)*atr
    return atr

def raw_mid(raw):
    idx=pd.to_datetime(raw.timestamp,unit='ms',utc=True)
    vol=(raw.volume_bid.to_numpy(float)+raw.volume_ask.to_numpy(float)) if 'volume_bid' in raw else np.zeros(len(raw))
    return pd.DataFrame({'open':(raw.bid_open.to_numpy(float)+raw.ask_open.to_numpy(float))/2,'high':(raw.bid_high.to_numpy(float)+raw.ask_high.to_numpy(float))/2,'low':(raw.bid_low.to_numpy(float)+raw.ask_low.to_numpy(float))/2,'close':(raw.bid_close.to_numpy(float)+raw.ask_close.to_numpy(float))/2,'volume':vol},index=idx)

def resample(x,rule):
    return pd.DataFrame({'open':x.open.resample(rule).first(),'high':x.high.resample(rule).max(),'low':x.low.resample(rule).min(),'close':x.close.resample(rule).last(),'volume':x.volume.resample(rule).sum()}).dropna(subset=['open','high','low','close'])

def partial_row(m1, event, rule):
    et=pd.Timestamp(event); start=et.floor(rule)
    a=m1.index.searchsorted(start,side='left'); b=m1.index.searchsorted(et,side='left')
    if b<=a: return None
    z=m1.iloc[a:b]
    return {'open':float(z.open.iloc[0]),'high':float(z.high.max()),'low':float(z.low.min()),'close':float(z.close.iloc[-1]),'volume':float(z.volume.sum())}

def make_frame_dict(vals, liq):
    price,e50,e200,rsi,macd,ms,mh,adx,pdi,mdi,atr,st,vr,bull,bear,_,_=vals
    ret=(liq.get('retest') or {}) if isinstance(liq,dict) else {}; state=ret.get('state'); sweep=liq.get('sweep') if isinstance(liq,dict) else None
    if state=='CONFIRMED':
        if sweep=='SELL_SIDE_SWEEP': bull+=15
        elif sweep=='BUY_SIDE_SWEEP': bear+=15
    elif sweep=='SELL_SIDE_SWEEP': bull+=10
    elif sweep=='BUY_SIDE_SWEEP': bear+=10
    else:
        bias=liq.get('bias') if isinstance(liq,dict) else None
        if bias in ('أقرب سيولة بيعية','سيولة بيعية تحت السعر'): bear+=5
        elif bias in ('أقرب سيولة شرائية','سيولة شرائية فوق السعر'): bull+=5
    direction=1 if bull>bear else -1 if bear>bull else 0; score=int(min(100,round(abs(bull-bear))))
    return {'price':price,'close':price,'ema50':e50,'ema200':e200,'rsi':rsi,'macd':macd,'macd_signal':ms,'macd_hist':mh,'adx':adx,'plus_di':pdi,'minus_di':mdi,'atr':max(atr,1e-9),'structure':'صاعد' if st==1 else 'هابط' if st==-1 else 'محايد','volume_ratio':vr,'bull':bull,'bear':bear,'score':score,'direction':'BUY' if direction==1 else 'SELL' if direction==-1 else 'WAIT','liquidity':liq}

def _market_open_event(ts):
    ts=pd.Timestamp(ts)
    if ts.tzinfo is None: ts=ts.tz_localize('UTC')
    else: ts=ts.tz_convert('UTC')
    wd=ts.weekday(); hour=ts.hour
    return not (wd==5 or (wd==6 and hour<22) or (wd==4 and hour>=21))

class FastLiquidity:
    """Numerically faithful, pandas-free port of v19.71 _liquidity_analysis_unlocked.
    It deliberately preserves the production state/index semantics, including the
    mixed local/global bar-index behavior in the original state machine.
    """
    def __init__(self):
        self.states=[]
        self.max_states=200
        self.disp_max=3
        self.bos_max=12
        self.retest_max=12
        self.max_age=36
        self.confirm_max=1

    def update(self, f, complete_pos, partial=None):
        # Reconstruct the exact logical input x = df.tail(80), then completed=x.iloc[:-1].
        # f carries a RangeIndex and the original openTime-equivalent timestamp index.
        if partial is None:
            st=max(0, complete_pos-78)
            vals=f.iloc[st:complete_pos+1][['open','high','low','close']]
            idx=np.arange(st, complete_pos+1, dtype=np.int64)
            t_idx=f.index[st:complete_pos+1]
            op=vals.open.to_numpy(float); hi=vals.high.to_numpy(float); lo=vals.low.to_numpy(float); cl=vals.close.to_numpy(float)
        else:
            st=max(0, complete_pos-77)
            vals=f.iloc[st:complete_pos+1][['open','high','low','close']]
            idx=np.arange(st, complete_pos+1, dtype=np.int64)
            t_idx=f.index[st:complete_pos+1]
            op=np.r_[vals.open.to_numpy(float), float(partial['open'])]
            hi=np.r_[vals.high.to_numpy(float), float(partial['high'])]
            lo=np.r_[vals.low.to_numpy(float), float(partial['low'])]
            cl=np.r_[vals.close.to_numpy(float), float(partial['close'])]
            idx=np.r_[idx, complete_pos]
            t_idx=list(t_idx)+[pd.Timestamp('2099-01-01', tz='UTC')]
        if len(op)<30:
            return {'bias':'محايدة','sweep':None,'retest':{'state':None},'score':0,'buy_side':[],'sell_side':[]}
        n=len(op); cn=n-1  # number of completed rows; last completed position = cn-1
        last=cn-1
        price=float(cl[-1]); atr=max(ewma_atr_last(hi,lo,cl),1e-9)
        radius=max(atr*.30,price*.00030)
        ch_arr=hi[:cn]; cl_arr=lo[:cn]
        highs=[]; lows=[]
        for i in range(2,cn-2):
            if ch_arr[i] >= np.max(ch_arr[i-2:i]) and ch_arr[i] >= np.max(ch_arr[i+1:i+3]): highs.append(float(ch_arr[i]))
            if cl_arr[i] <= np.min(cl_arr[i-2:i]) and cl_arr[i] <= np.min(cl_arr[i+1:i+3]): lows.append(float(cl_arr[i]))
        def cluster(vals):
            groups=[]
            for v in sorted(vals):
                if not groups or abs(v-(sum(groups[-1])/len(groups[-1]))) > radius: groups.append([v])
                else: groups[-1].append(v)
            return [round(sum(g)/len(g),2) for g in groups]
        buy=sorted(v for v in cluster(highs) if v>price); sell=sorted((v for v in cluster(lows) if v<price), reverse=True)
        nb=buy[0] if buy else None; ns=sell[0] if sell else None
        # Exact production: prior = completed.iloc[:-1].tail(20), i.e.
        # exclude the current completed candle itself before the 20-bar lookback.
        prior_start=max(0,cn-21)
        prior_stop=last
        ph=float(np.max(hi[prior_start:prior_stop])); pl=float(np.min(lo[prior_start:prior_stop]))
        ch=float(hi[last]); clo=float(lo[last]); cc=float(cl[last]);
        sweep=None; sweep_level=None; score=0; factors=[]
        current_idx=int(idx[last])
        if ch>ph and cc<ph:
            sweep='BUY_SIDE_SWEEP'; sweep_level=ph; score=-8; factors.append('سحب سيولة شرائية')
        elif clo<pl and cc>pl:
            sweep='SELL_SIDE_SWEEP'; sweep_level=pl; score=8; factors.append('سحب سيولة بيعية')
        try:
            secs=max(1,int(np.median(np.diff(np.array([pd.Timestamp(x).value for x in t_idx]))/1e9)))
            tf_key=f'{secs}s'
        except Exception:
            tf_key='unknown'
        if sweep is not None:
            key=f'{tf_key}:{current_idx}:{round(float(sweep_level),4)}:{sweep}'
            if not any(stt.get('key')==key for stt in self.states):
                self.states.append({
                    'key':key,'sweep':sweep,'level':float(sweep_level),'sweep_time':str(current_idx),
                    'sweep_bar_index':last,'status':'SWEEP','retest_status':'بانتظار الاندفاع بعد سحب السيولة',
                    'retest_level':float(sweep_level),'retest_time':None,'retest_price':None,'retest_result':None,
                    'last_time':str(current_idx),'last_bar_index':cn,'displacement':False,'displacement_time':None,
                    'bos':False,'bos_time':None,'retest_start_index':None,'confirmed_bar_index':None,
                    'invalidated_reason':None,'tf_key':tf_key})
        terminal={'CONSUMED','EXPIRED','INVALIDATED'}
        for stt in self.states:
            if stt.get('tf_key')!=tf_key or stt.get('status') in terminal: continue
            age=last-int(stt.get('sweep_bar_index',last)); stt['age_bars']=max(0,age); stt['last_bar_index']=cn; stt['last_time']=str(current_idx)
            if age>self.max_age:
                stt['status']='EXPIRED'; stt['retest_status']='انتهى عمر حالة السيولة'; stt['retest_result']='EXPIRED'; stt['invalidated_reason']='MAX_AGE'
        candidates=[stt for stt in self.states if stt.get('tf_key')==tf_key and stt.get('status') not in terminal]
        active=max(candidates,key=lambda z:int(z.get('sweep_bar_index',-1))) if candidates else None
        if active:
            sweep_pos=int(active.get('sweep_bar_index',-1))
            if sweep_pos<0 or sweep_pos>=cn:
                active['status']='EXPIRED'; active['retest_status']='انتهى عمر الحالة'; active['retest_result']='EXPIRED'; active['invalidated_reason']='INVALID_INDEX'; active=None
        if active:
            level=float(active['level']); age=last-int(active.get('sweep_bar_index',last)); active['age_bars']=max(0,age)
            tol=max(atr*.18,price*.00020,.20)
            after_start=int(active['sweep_bar_index'])+1
            if active['status']=='SWEEP':
                checked=min(self.disp_max, max(0,last-after_start+1))
                found=False
                for j in range(checked):
                    pos=after_start+j
                    oh=float(op[pos]); clo2=float(cl[pos]); hi2=float(hi[pos]); lo2=float(lo[pos]); rng=max(hi2-lo2,1e-9); br=abs(clo2-oh)/rng
                    away=(clo2>level+tol*.25) if active['sweep']=='SELL_SIDE_SWEEP' else (clo2<level-tol*.25)
                    if br>=.60 and rng>=atr*.75 and away:
                        active['displacement']=True; active['displacement_time']=str(int(idx[pos])); active['displacement_bar_index']=int(active['sweep_bar_index'])+1+j; active['status']='DISPLACEMENT'; active['retest_status']='بانتظار تأكيد كسر الهيكل'; found=True; break
                if not found and age>=self.disp_max:
                    active['status']='EXPIRED'; active['retest_status']='انتهى وقت الاندفاع'; active['retest_result']='EXPIRED'; active['invalidated_reason']='DISPLACEMENT_TIMEOUT'
            if active and active['status']=='DISPLACEMENT':
                a=int(active['sweep_bar_index']); pre_lo=max(0,a-20); pre_hi=a
                if pre_hi-pre_lo>=4:
                    phh=float(np.max(hi[pre_lo:pre_hi])); pll=float(np.min(lo[pre_lo:pre_hi])); start=int(active.get('displacement_bar_index',a+1)); end=min(cn,start+1+self.bos_max)
                    found=False
                    for pos in range(start+1,end):
                        close=float(cl[pos])
                        if active['sweep']=='SELL_SIDE_SWEEP' and close>phh:
                            active['bos']=True; active['bos_time']=str(int(idx[pos])); active['bos_bar_index']=int(idx[pos]); active['status']='BOS'; active['retest_status']='بانتظار إعادة الاختبار'; active['retest_start_index']=active['bos_bar_index']+1; found=True; break
                        if active['sweep']=='BUY_SIDE_SWEEP' and close<pll:
                            active['bos']=True; active['bos_time']=str(int(idx[pos])); active['bos_bar_index']=int(idx[pos]); active['status']='BOS'; active['retest_status']='بانتظار إعادة الاختبار'; active['retest_start_index']=active['bos_bar_index']+1; found=True; break
                if active and active['status']=='DISPLACEMENT':
                    disp_age=last-int(active.get('displacement_bar_index',a))
                    if disp_age>self.bos_max:
                        active['status']='EXPIRED'; active['retest_status']='انتهى وقت تأكيد كسر الهيكل'; active['retest_result']='EXPIRED'; active['invalidated_reason']='BOS_TIMEOUT'
            if active and active['status']=='BOS':
                rs=int(active.get('retest_start_index',cn+1)); retest_len=max(0,last-rs+1); retest_age=last-int(active.get('bos_bar_index',last))
                if retest_age>self.retest_max:
                    active['status']='EXPIRED'; active['retest_status']='انتهى وقت إعادة الاختبار'; active['retest_result']='EXPIRED'; active['invalidated_reason']='RETEST_TIMEOUT'
                elif retest_len:
                    active['status']='RETEST'; pos=last; lc=float(cl[pos]); lh=float(hi[pos]); ll=float(lo[pos]);
                    if active['sweep']=='SELL_SIDE_SWEEP':
                        touched=ll<=level+tol and lh>=level-tol; rejected=touched and lc>level and (level-ll)>=tol*.25; invalid=lc<level-tol
                    else:
                        touched=lh>=level-tol and ll<=level+tol; rejected=touched and lc<level and (lh-level)>=tol*.25; invalid=lc>level+tol
                    active['retest_time']=str(int(idx[pos])); active['retest_price']=lc; active['last_time']=str(int(idx[pos]))
                    if invalid:
                        active['status']='INVALIDATED'; active['retest_status']='إعادة الاختبار فاشلة — تم إبطال المستوى'; active['retest_result']='FAILED'; active['invalidated_reason']='RETEST_INVALIDATED'
                    elif rejected:
                        active['status']='CONFIRMED'; active['retest_status']='إعادة الاختبار ناجحة'; active['retest_result']='SUCCESS'; active['confirmed_bar_index']=cn
                    elif touched:
                        active['status']='RETEST'; active['retest_status']='إعادة الاختبار قيد التقييم'
            if active and active['status']=='RETEST':
                retest_age=last-int(active.get('bos_bar_index',last))
                if retest_age>self.retest_max:
                    active['status']='EXPIRED'; active['retest_status']='انتهى وقت إعادة الاختبار'; active['retest_result']='EXPIRED'; active['invalidated_reason']='RETEST_TIMEOUT'
            if active and active['status']=='CONFIRMED':
                confirmed_age=last-int(active.get('confirmed_bar_index',last))
                if confirmed_age>self.confirm_max:
                    active['status']='CONSUMED'; active['retest_status']='تم استهلاك حالة إعادة الاختبار'; active['retest_result']='CONSUMED'
        if len(self.states)>self.max_states:
            self.states=sorted(self.states,key=lambda stt:int(stt.get('sweep_bar_index',-1)))[-self.max_states:]
        active_candidates=[stt for stt in self.states if stt.get('tf_key')==tf_key and stt.get('status') not in terminal]
        active=max(active_candidates,key=lambda z:int(z.get('sweep_bar_index',-1))) if active_candidates else None
        if active:
            state=active.get('status')
            if state=='CONFIRMED': score += 12 if active['sweep']=='SELL_SIDE_SWEEP' else -12
            factors.append({'RETEST':'إعادة الاختبار قيد التقييم','BOS':'كسر الهيكل مؤكد — بانتظار إعادة الاختبار','DISPLACEMENT':'الاندفاع مؤكد — بانتظار كسر الهيكل','SWEEP':'سحب السيولة مؤكد — بانتظار الاندفاع'}.get(state,''))
        else:
            latest=[stt for stt in self.states if stt.get('tf_key')==tf_key]
        if nb is not None and ns is not None:
            up=nb-price; down=price-ns; bias='أقرب سيولة شرائية' if up<down*.75 else 'أقرب سيولة بيعية' if down<up*.75 else 'متوازنة'
        elif nb is not None: bias='سيولة شرائية فوق السعر'
        elif ns is not None: bias='سيولة بيعية تحت السعر'
        else: bias='محايدة'
        if sweep=='BUY_SIDE_SWEEP': bias='سحب سيولة شرائية'
        elif sweep=='SELL_SIDE_SWEEP': bias='سحب سيولة بيعية'
        retest={
            'status':active.get('retest_status','لا توجد حالة نشطة') if active else 'لا توجد حالة نشطة',
            'level':active.get('level') if active else None,'state':active.get('status') if active else None,
            'displacement':bool(active.get('displacement')) if active else False,'bos':bool(active.get('bos')) if active else False,
            'age_bars':active.get('age_bars',0) if active else 0,'sweep_time':active.get('sweep_time') if active else None,
            'sweep_bar_index':active.get('sweep_bar_index') if active else None,'bos_time':active.get('bos_time') if active else None,
            'key':active.get('key') if active else None}
        if not active:
            terminal_states=[stt for stt in self.states if stt.get('tf_key')==tf_key]
            if terminal_states:
                last=max(terminal_states,key=lambda z:int(z.get('sweep_bar_index',-1)))
                retest.update({'status':last.get('retest_status'),'level':last.get('level'),'state':last.get('status'),'displacement':bool(last.get('displacement')),'bos':bool(last.get('bos')),'age_bars':last.get('age_bars',0),'sweep_time':last.get('sweep_time'),'sweep_bar_index':last.get('sweep_bar_index'),'bos_time':last.get('bos_time'),'key':last.get('key')})
        sw=sweep or (active.get('sweep') if active else None)
        return {'bias':bias,'sweep':sw,'retest':retest,'score':score,'buy_side':buy[:3],'sell_side':sell[:3],'nearest_buy':nb,'nearest_sell':ns}

def pivots(f,complete_pos,partial,n=120):
    # exact support_resistance uses tail(120) including current partial bar
    start=max(0,complete_pos-(n-2)); base=f.iloc[start:complete_pos+1].copy()
    if partial is not None:
        base=pd.concat([base[['open','high','low','close','volume']],pd.DataFrame([partial],index=[pd.Timestamp('2099-01-01')])])
    else: base=base[['open','high','low','close','volume']]
    current=float(base.close.iloc[-1]); atr=ewma_atr_last(base.high.to_numpy(float),base.low.to_numpy(float),base.close.to_numpy(float)); radius=max(atr*.35,current*.00035)
    h=base.high.to_numpy(float); l=base.low.to_numpy(float); supports=[]; resistances=[]
    for i in range(2,len(base)-2):
        if l[i]<=l[i-2:i].min() and l[i]<=l[i+1:i+3].min() and l[i]<current: supports.append(float(l[i]))
        if h[i]>=h[i-2:i].max() and h[i]>=h[i+1:i+3].max() and h[i]>current: resistances.append(float(h[i]))
    def cluster(vals):
        groups=[]
        for p in sorted(vals):
            if not groups or abs(p-np.mean(groups[-1]))>radius: groups.append([p])
            else: groups[-1].append(p)
        return [float(np.mean(g)) for g in groups]
    s=sorted(cluster(supports),key=lambda z:abs(current-z))[:3]; r=sorted(cluster(resistances),key=lambda z:abs(current-z))[:3]
    return {'support1':s[0] if len(s)>0 else None,'support2':s[1] if len(s)>1 else None,'support3':s[2] if len(s)>2 else None,'resistance1':r[0] if len(r)>0 else None,'resistance2':r[1] if len(r)>1 else None,'resistance3':r[2] if len(r)>2 else None,'atr':atr}

def scenario_type(direction,frames,levels,price):
    if direction not in ('BUY','SELL'): return 'UNKNOWN'
    primary=frames.get('M15') or frames.get('H1') or {}
    liq=primary.get('liquidity',{}) or {}; sweep=liq.get('sweep'); wanted='SELL_SIDE_SWEEP' if direction=='BUY' else 'BUY_SIDE_SWEEP'; state=(liq.get('retest',{}) or {}).get('state') or 'NONE'
    if sweep==wanted and state in ('SWEEP','DISPLACEMENT','BOS','RETEST','CONFIRMED'): return 'LIQUIDITY_REVERSAL'
    structure=primary.get('structure'); close=primary.get('close',price)
    resist=[v for k,v in levels.items() if k.startswith('resistance') and v is not None and v>price]; supp=[v for k,v in levels.items() if k.startswith('support') and v is not None and v<price]
    resistance=min(resist) if resist else None; support=max(supp) if supp else None
    if direction=='BUY' and structure=='صاعد' and resistance is not None and float(close or price)>=float(resistance): return 'BREAKOUT'
    if direction=='SELL' and structure=='هابط' and support is not None and float(close or price)<=float(support): return 'BREAKOUT'
    directions=[x.get('direction') for x in frames.values() if isinstance(x,dict)]
    same=sum(1 for d in directions if d==direction)
    return 'CONTINUATION' if same>=max(2,len(directions)-1) else 'REVERSAL'

def scenario_score(frames,levels,price,atr,direction):
    weights={'H1':30,'M15':25,'M5':20}; names=['H1','M15','M5']; total=75.0
    tr=sum(weights[n] for n in names if frames[n]['direction']==direction)/total; trend=20 if tr>=.9 else 15 if tr>=.6 else 8 if tr>=.3 else 0
    exp='صاعد' if direction=='BUY' else 'هابط'; sr=sum(weights[n] for n in names if frames[n]['structure']==exp)/total; structure=20 if sr>=.9 else 15 if sr>=.6 else 8 if sr>=.3 else 0
    # v19.71 daily Scenario liquidity source is H1; Trigger maturity source is M15.
    hliq=frames['H1'].get('liquidity',{}) or {}; hstate=(hliq.get('retest') or {}).get('state') or 'NONE'; hsw=hliq.get('sweep'); want='SELL_SIDE_SWEEP' if direction=='BUY' else 'BUY_SIDE_SWEEP'; lq=0
    if hsw==want: lq=10 if hstate not in ('DISPLACEMENT','BOS','RETEST','CONFIRMED') else 15 if hstate in ('DISPLACEMENT','BOS') else 18
    m=frames['M15']; adx=m['adx']; p=m['plus_di']; md=m['minus_di']; ml=m['macd']; ms=m['macd_signal']; rsi=m['rsi']; mom=0
    if adx>=25 and ((direction=='BUY' and p>md) or (direction=='SELL' and md>p)): mom+=7
    elif adx>=20 and ((direction=='BUY' and p>md) or (direction=='SELL' and md>p)): mom+=5
    if (direction=='BUY' and ml>ms) or (direction=='SELL' and ml<ms): mom+=5
    if (direction=='BUY' and rsi>=50) or (direction=='SELL' and rsi<=50): mom+=3
    mliq=m.get('liquidity',{}) or {}; msweep=mliq.get('sweep')==want; mstate=(mliq.get('retest') or {}).get('state') or 'NONE'; trigger=15 if mstate=='CONFIRMED' else 10 if mstate=='RETEST' else 5 if mstate=='BOS' else 2 if mstate=='DISPLACEMENT' else 0
    if msweep and trigger<0: trigger=0
    resist=[v for k,v in levels.items() if k.startswith('resistance') and v is not None and v>price]; supp=[v for k,v in levels.items() if k.startswith('support') and v is not None and v<price]
    obstacle=min(resist) if direction=='BUY' and resist else max(supp) if direction=='SELL' and supp else None
    anchor=max(supp) if direction=='BUY' and supp else min(resist) if direction=='SELL' and resist else None
    scenario=scenario_type(direction,frames,levels,price); space=abs(float(obstacle)-float(price))/max(atr,1e-9) if obstacle is not None else 2.0; ad=abs(float(price)-float(anchor))/max(atr,1e-9) if anchor is not None else 99.0
    loc=10 if obstacle is None or space>=2 else 7 if space>=1.2 else 4 if space>=.7 else 0
    if scenario in ('REVERSAL','LIQUIDITY_REVERSAL') and anchor is not None and ad<=.25: loc=max(loc,7)
    elif scenario=='CONTINUATION' and anchor is not None and ad<=.25: loc=min(loc,5)
    return int(max(0,min(100,trend+structure+lq+mom+trigger+loc)))

def build_trade(direction,h1,m15,levels):
    entry=float(m15['price']); atr=max(float(m15['atr']),float(h1['atr']) if float(m15['atr'])<=0 else 0,0.50)
    sup=sorted([float(levels[k]) for k in ('support1','support2','support3') if levels.get(k) is not None and levels[k]<entry],reverse=True); res=sorted([float(levels[k]) for k in ('resistance1','resistance2','resistance3') if levels.get(k) is not None and levels[k]>entry])
    mn=max(atr*.8,entry*.00035); mx=max(atr*3,mn*1.10)
    if direction=='BUY':
        sl=entry-mn
        if sup:
            x=sup[0]-atr*.15; rr=entry-x
            if mn<=rr<=mx: sl=x
        risk=entry-sl; t1=entry+risk*1.2; t2=entry+risk*1.8; t3=entry+risk*2.4
        if res and t1<=res[0]<=entry+atr*2: t1=res[0]
        if len(res)>=2 and t2<=res[1]<=entry+atr*3: t2=res[1]
        if len(res)>=3 and t3<=res[2]<=entry+atr*4: t3=res[2]
        t1=max(t1,entry+risk*1.2); t2=max(t2,t1+atr*.25,entry+risk*1.8); t3=max(t3,t2+atr*.35,entry+risk*2.4)
        valid=entry>sl and sl<t1<t2<t3
    else:
        sl=entry+mn
        if res:
            x=res[0]+atr*.15; rr=x-entry
            if mn<=rr<=mx: sl=x
        risk=sl-entry; t1=entry-risk*1.2; t2=entry-risk*1.8; t3=entry-risk*2.4
        if sup and entry-atr*2<=sup[0]<=t1: t1=sup[0]
        if len(sup)>=2 and entry-atr*3<=sup[1]<=t2: t2=sup[1]
        if len(sup)>=3 and entry-atr*4<=sup[2]<=t3: t3=sup[2]
        t1=min(t1,entry-risk*1.2); t2=min(t2,t1-atr*.25,entry-risk*1.8); t3=min(t3,t2-atr*.35,entry-risk*2.4)
        valid=entry<sl and sl>t1>t2>t3
    if not valid: return None
    rr=abs(t3-entry)/abs(entry-sl)
    if rr<1.20: return None
    return {'entry':round(entry,2),'sl':round(sl,2),'tp1':round(t1,2),'tp2':round(t2,2),'tp3':round(t3,2),'rr':round(rr,2)}

def main():
    t0=time.time(); print('load 2024',flush=True); raw=pd.read_csv(M1_PATH); m124=raw_mid(raw)
    # Warmup archive has no volume columns; zeros are equivalent to unavailable tick volume for indicator warmup, while 2024 keeps real volumes.
    if WARM_PATH.exists():
        rw=pd.read_csv(WARM_PATH); rw['volume_bid']=0; rw['volume_ask']=0; rw=rw[pd.to_datetime(rw.timestamp,unit='ms',utc=True)>=pd.Timestamp('2023-10-01',tz='UTC')]
        warm=raw_mid(rw); m1=pd.concat([warm,m124]); m1=m1[~m1.index.duplicated(keep='last')].sort_index()
    else: m1=m124
    m5=resample(m1,'5min'); m15=resample(m1,'15min'); h1=resample(m1,'1h'); print('bars',len(m5),len(m15),len(h1),'warm rows',len(m1),flush=True)
    # Event = next scan boundary after each M15 bar. Use all 2024 M15 buckets as scan events and enforce production market-open rule.
    events=[x+pd.Timedelta(minutes=15) for x in m15.index if pd.Timestamp('2024-01-01',tz='UTC')<=x<=pd.Timestamp('2024-12-31 21:45',tz='UTC') and _market_open_event(x+pd.Timedelta(minutes=15))]
    maxe=int(os.environ.get('MAX_EVENTS','0') or 0); events=events[:maxe] if maxe else events
    # Positional maps for full bars.
    h1_idx=h1.index; m5_idx=m5.index; m15_idx=m15.index; m1idx=m124.index
    L1=FastLiquidity(); L5=FastLiquidity(); L15=FastLiquidity(); decisions=[]; candidates=[]
    for i,et in enumerate(events):
        et=pd.Timestamp(et)
        # Work windows after analyze() drops final returned bar.
        q1=h1_idx.searchsorted(et.floor('1h'),side='left')-1
        q5=m5_idx.searchsorted(et.floor('5min'),side='left')-1
        q15=m15_idx.searchsorted(et.floor('15min')-pd.Timedelta(minutes=15),side='right')-1
        a1=q1; a5=q5; a15=q15-1
        p1_probe=partial_row(m1,et,'1h'); p5_probe=partial_row(m1,et,'5min')
        if a1 < 28 or a5 < 29 or a15 < 29: continue
        s1=max(0,a1-298); s5=max(0,a5-298); s15=max(0,a15-298)
        o1=h1.iloc[s1:a1+1].open.to_numpy(float); h_1=h1.iloc[s1:a1+1].high.to_numpy(float); l1=h1.iloc[s1:a1+1].low.to_numpy(float); c1=h1.iloc[s1:a1+1].close.to_numpy(float); v1=h1.iloc[s1:a1+1].volume.to_numpy(float)
        o5=m5.iloc[s5:a5+1].open.to_numpy(float); h5=m5.iloc[s5:a5+1].high.to_numpy(float); l5=m5.iloc[s5:a5+1].low.to_numpy(float); c5=m5.iloc[s5:a5+1].close.to_numpy(float); v5=m5.iloc[s5:a5+1].volume.to_numpy(float)
        o15=m15.iloc[s15:a15+1].open.to_numpy(float); h15=m15.iloc[s15:a15+1].high.to_numpy(float); l15=m15.iloc[s15:a15+1].low.to_numpy(float); c15=m15.iloc[s15:a15+1].close.to_numpy(float); v15=m15.iloc[s15:a15+1].volume.to_numpy(float)
        p1=partial_row(m1,et,'1h'); p5=partial_row(m1,et,'5min')
        liq1=L1.update(h1,q1,p1); liq5=L5.update(m5,q5,p5); liq15=L15.update(m15,q15,None)
        # Exact analyze() receives the current H1/M5 forming candle but removes it
        # before indicator/scoring calculations. Reproduce that causally: include
        # the partial only for the >=30 readiness condition, then calculate on the
        # completed prefix. M15 has no forming row here; its latest closed row is
        # likewise removed by analyze(), so a15 already points to the prior close.
        A1=[o1,h_1,l1,c1,v1]; A5=[o5,h5,l5,c5,v5]
        if p1 is not None:
            for k,z in enumerate((p1['open'],p1['high'],p1['low'],p1['close'],p1['volume'])): A1[k]=np.r_[A1[k],float(z)]
        if p5 is not None:
            for k,z in enumerate((p5['open'],p5['high'],p5['low'],p5['close'],p5['volume'])): A5[k]=np.r_[A5[k],float(z)]
        if len(A1[0])<30 or len(A5[0])<30 or len(o15)<30: continue
        def analyze_like_arrays(A):
            # V19.71 analyze(): if len>=31, remove the last forming candle;
            # if exactly 30, it deliberately falls back to the full frame.
            q=[np.asarray(x) for x in A]
            if len(q[0])>=31:
                q=[x[:-1] for x in q]
            return frame_calc(q[0],q[1],q[2],q[3],q[4])
        F1=make_frame_dict(analyze_like_arrays(A1),liq1)
        F5=make_frame_dict(analyze_like_arrays(A5),liq5)
        F15=make_frame_dict(frame_calc(o15,h15,l15,c15,v15),liq15)
        price=float(m124.close.iloc[m1idx.searchsorted(et,side='left')-1])
        levels=pivots(h1,q1,p1,120)
        frames={'H1':F1,'M15':F15,'M5':F5}; bs=scenario_score(frames,levels,price,F15['atr'],'BUY'); ss=scenario_score(frames,levels,price,F15['atr'],'SELL'); direction='BUY' if bs>ss else 'SELL' if ss>bs else 'WAIT'; sc=max(bs,ss)
        lstate=(F15['liquidity'].get('retest') or {}).get('state'); blocked=lstate=='INVALIDATED'; setup=float(sc)
        cov=sum(f['direction']==direction for f in (F1,F15,F5)) if direction!='WAIT' else 0; opp=sum(f['direction'] not in (direction,'WAIT') for f in (F1,F15,F5)) if direction!='WAIT' else 0
        if cov<2: setup-=15
        if opp: setup-=10*opp
        adx=F15['adx']; setup-=20 if adx<15 else 10 if adx<20 else 0
        if direction!='WAIT' and F1['structure'] in ('صاعد','هابط') and F1['structure']!=('صاعد' if direction=='BUY' else 'هابط'): setup-=10
        cand=build_trade(direction,F1,F15,levels) if direction!='WAIT' else None; rr_ok=bool(cand and cand['rr']>=1.2); ready=bool(direction!='WAIT' and setup>=50 and cov==3 and adx>=15 and not blocked and rr_ok)
        row={'event_time':et.isoformat(),'direction':direction,'signal':ready,'score':int(np.clip(round(setup),0,100)),'scenario_score':sc,'H1':F1['direction'],'M15':F15['direction'],'M5':F5['direction'],'h1_score':F1['score'],'m15_score':F15['score'],'m5_score':F5['score'],'m15_adx':adx,'liquidity_state':lstate,'liquidity_sweep_m15':F15['liquidity'].get('sweep'),'liquidity_state_h1':(F1['liquidity'].get('retest') or {}).get('state'),'liquidity_sweep_h1':F1['liquidity'].get('sweep'),'entry':cand['entry'] if cand else None,'sl':cand['sl'] if cand else None,'tp1':cand['tp1'] if cand else None,'tp2':cand['tp2'] if cand else None,'tp3':cand['tp3'] if cand else None,'rr':cand['rr'] if cand else None,'m15_atr':F15['atr'],'live_price':price}
        decisions.append(row)
        if ready and cand: candidates.append(row.copy())
        if i and i%1000==0: print('events',i,'signals',len(candidates),'sec',round(time.time()-t0,2),flush=True)

    # Event-driven production-like lifecycle: update active trades only with the
    # newly available M1 bars since each trade's last state update, then register
    # the current signal using the same ACTIVE/TP1/TP2 dedupe contract.
    times=m124.index.astype('int64').to_numpy()
    hi=m124.high.to_numpy(float); lo=m124.low.to_numpy(float)
    mid_hi=m124.high.to_numpy(float); mid_lo=m124.low.to_numpy(float)
    active=[]; all_trades=[]

    def advance_trade(tr, until_ns):
        if tr.get('status') not in ('ACTIVE','TP1','TP2'): return
        start_ns=int(tr.get('_last_update_ns',0))
        a=np.searchsorted(times,start_ns,side='right')
        b=np.searchsorted(times,until_ns,side='left')  # never consume the current event minute
        if b<=a: return
        d=tr['direction']; entry=float(tr['entry']); sl=float(tr['sl']); t1=float(tr['tp1']); t2=float(tr['tp2']); t3=float(tr['tp3'])
        state=tr.get('status','ACTIVE')
        mfe=float(tr.get('mfe',0.0)); mae=float(tr.get('mae',0.0)); risk=float(tr.get('risk',abs(entry-sl)))
        for j in range(a,b):
            hh=float(mid_hi[j]); ll=float(mid_lo[j]); bar_ns=int(times[j]); stamp=pd.Timestamp(bar_ns,unit='ns',tz='UTC').isoformat()
            if d=='BUY':
                mfe=max(mfe,hh-entry); mae=min(mae,ll-entry); sl_hit=ll<=sl; t1hit=hh>=t1; t2hit=hh>=t2; t3hit=hh>=t3
            else:
                mfe=max(mfe,entry-ll); mae=min(mae,entry-hh); sl_hit=hh>=sl; t1hit=ll<=t1; t2hit=ll<=t2; t3hit=ll<=t3
            if sl_hit:
                tr.update({'status':'CLOSED','result':'LOSS','close_time':stamp,'tp1_hit':bool(tr.get('tp1_hit',False)),'tp2_hit':bool(tr.get('tp2_hit',False)),'tp3_hit':False,'mfe':mfe,'mae':abs(mae),'_last_update_ns':bar_ns})
                return
            if t3hit:
                tr.update({'status':'CLOSED','result':'TP3 / WIN','close_time':stamp,'tp1_hit':True,'tp2_hit':True,'tp3_hit':True,'mfe':mfe,'mae':abs(mae),'tp1_time':tr.get('tp1_time') or stamp,'tp2_time':tr.get('tp2_time') or stamp,'tp3_time':stamp,'_last_update_ns':bar_ns})
                return
            if t2hit and state in ('ACTIVE','TP1'):
                tr['tp1_hit']=True; tr['tp2_hit']=True; tr['tp1_time']=tr.get('tp1_time') or stamp; tr['tp2_time']=stamp; state='TP2'; tr['status']='TP2'; tr['_last_update_ns']=bar_ns
            elif t1hit and state=='ACTIVE':
                tr['tp1_hit']=True; tr['tp1_time']=tr.get('tp1_time') or stamp; state='TP1'; tr['status']='TP1'; tr['_last_update_ns']=bar_ns
            tr['mfe']=mfe; tr['mae']=abs(mae)
        tr['_last_update_ns']=int(times[b-1])

    for i,r in enumerate(decisions):
        et=pd.Timestamp(r['event_time'])
        et_ns=et.value
        for tr in active:
            advance_trade(tr,et_ns)
        if not r.get('signal') or r.get('entry') is None:
            continue
        rec={**r,'status':'ACTIVE','result':'OPEN','time':r['event_time'],'last_update':r['event_time'],'close_time':None,'tp1_time':None,'tp2_time':None,'tp3_time':None,'tp1_hit':False,'tp2_hit':False,'tp3_hit':False,'mfe':0.0,'mae':0.0,'risk':abs(float(r['entry'])-float(r['sl'])),'_last_update_ns':et_ns}
        atr=max(float(r.get('m15_atr',0) or 0),.50); price=float(r.get('live_price',r['entry'])); entry=float(r['entry']); sl=float(r['sl']); t1=float(r['tp1']); t2=float(r['tp2']); t3=float(r['tp3'])
        e_tol=max(atr*.30,price*.00025,.50); s_tol=max(atr*.45,price*.00035,.75); p_tol=max(atr*.55,price*.00045,1.0)
        duplicate=False
        for x in active[::-1]:
            if x.get('status') not in ('ACTIVE','TP1','TP2') or x.get('direction')!=r['direction']: continue
            if abs(float(x['entry'])-entry)>e_tol or abs(float(x['sl'])-sl)>s_tol: continue
            if abs(float(x['tp1'])-t1)>p_tol or abs(float(x['tp2'])-t2)>p_tol*1.5 or abs(float(x['tp3'])-t3)>p_tol*2: continue
            duplicate=True; break
        if not duplicate:
            active.append(rec); all_trades.append(rec)
        if (i+1)%1000==0:
            print('lifecycle',i+1,'registered',len(all_trades),'active',sum(x.get('status') in ('ACTIVE','TP1','TP2') for x in active),'sec',round(time.time()-t0,2),flush=True)

    final_ns=int(times[-1])+60_000_000_000
    for tr in active:
        if tr.get('status') in ('ACTIVE','TP1','TP2'):
            advance_trade(tr,final_ns)

    rdf=pd.DataFrame(all_trades)
    dec=pd.DataFrame(decisions)
    dec.to_csv(DEC_PATH,index=False); rdf.to_csv(OUT_PATH,index=False)
    closed=rdf[rdf.status=='CLOSED'].copy() if not rdf.empty else rdf
    wins=int((closed.result=='TP3 / WIN').sum()) if not closed.empty else 0
    losses=int((closed.result=='LOSS').sum()) if not closed.empty else 0
    unr=int(sum(rdf.status.isin(['ACTIVE','TP1','TP2']))) if not rdf.empty else 0
    if not rdf.empty:
        rdf['final_R']=np.where(rdf.result.eq('TP3 / WIN'),abs(rdf.tp3.astype(float)-rdf.entry.astype(float))/rdf.risk.astype(float),np.where(rdf.result.eq('LOSS'),-1.0,np.nan))
        rdf['mfe_R']=rdf.mfe.astype(float)/rdf.risk.astype(float); rdf['mae_R']=rdf.mae.astype(float)/rdf.risk.astype(float)
        closed=rdf[rdf.status=='CLOSED'];
    report={'baseline':'XAU_SMART_TRADER_v19_71_GY_FIXED_TP_500_750_1000.py','scope':'ordinary MAIN technical replay; institutional layer excluded from causal historical replay and frozen UNAVAILABLE (0 adjustment)','data_rows_2024':len(m124),'warmup_rows':len(m1)-len(m124),'events':len(events),'decision_rows':len(dec),'technical_ready':len(decisions) and int(sum(1 for x in decisions if x.get('signal'))) or 0,'accepted_after_register_like_dedupe':len(rdf),'SL':losses,'TP3':wins,'UNRESOLVED':unr,'win_rate_tp3_before_SL_pct':100*wins/(wins+losses) if wins+losses else None,'avg_R_closed':float(closed.final_R.mean()) if not closed.empty else None,'sum_R_closed':float(closed.final_R.sum()) if not closed.empty else None,'profit_factor':float(closed.loc[closed.final_R>0,'final_R'].sum()/(-closed.loc[closed.final_R<0,'final_R'].sum())) if not closed.empty and (closed.final_R<0).any() else None,'avg_MFE_R':float(rdf.mfe_R.mean()) if not rdf.empty else None,'avg_MAE_R':float(rdf.mae_R.mean()) if not rdf.empty else None,'elapsed_sec':time.time()-t0}
    Path(REPORT_PATH).write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8'); print(json.dumps(report,ensure_ascii=False,indent=2))

if __name__=='__main__': main()
