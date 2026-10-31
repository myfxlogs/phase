// dcd.go —— dcd 臂+差扫岗装配（T-0102——research 域零 venue 依赖红线：
// feed/probe 闭包在此以 okcli 落地，解析/分类逻辑在域内纯函数。
// T-0170 认证读面：闭包参数换 authGetter 窄面——无凭据件与 readcred 持守件
// （401 自重取）同形透明）。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"o/internal/platform/config"
	"o/internal/platform/notify"
	"o/internal/platform/rejects"

	"o/internal/research/dcd"
	"o/internal/research/funding"
	"o/internal/research/gapscan"
	"o/internal/research/grammar"
	"o/internal/research/hypothesis"
	"o/internal/research/researcher"
	"o/internal/research/sigdist"
	"o/internal/trading/okxexec"
)

// dcdFeedFor 双币赢枚举 REST 面（<endpoint_family>/currency-pair → products?uly=
// 全对在架；信封数字 code 由 okxexec flexCode 兼容——坑条 okx-dcd-endpoint-dialect）。
// 非价格源：本面=静态条款/目录注册（strike/expiry/apr）——价格消费只准
// WS 流径行（T-0171 价格源律）。
// get429 限频退避重读件（T-0117 根修：boot 两臂同打 DCD 产品端点撞 429
// fail-closed 让位成静默死亡——429/5xx ≤3 次指数退避重试再让位；4xx 不重试）。
func get429(ctx context.Context, okcli authGetter, path string) ([]byte, error) {
	var raw []byte
	var err error
	for try := 0; try < 4; try++ {
		raw, err = okcli.Get(ctx, path)
		if err == nil {
			return raw, nil
		}
		msg := err.Error()
		if !strings.Contains(msg, "429") && !strings.Contains(msg, "http 5") {
			return nil, err // 非限频/服务端错不重试
		}
		if try == 3 {
			break
		}
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(time.Duration(1<<try) * time.Second): // sched-exempt-ok：429 退避
		}
	}
	return nil, err
}

func dcdFeedFor(okcli authGetter, rej *rejects.Door) dcd.Feed {
	return func(ctx context.Context, s dcd.Surface) ([]dcd.Product, error) {
		if okcli == nil {
			return nil, fmt.Errorf("dcd feed 缺席（凭据缺）")
		}
		raw, err := get429(ctx, okcli, s.EndpointFamily+"/currency-pair")
		if err != nil {
			return nil, err
		}
		pairs, err := dcd.ParseCurrencyPairs(raw)
		if err != nil {
			return nil, err
		}
		var out []dcd.Product
		var fail int
		for i, pr := range pairs {
			if i > 0 {
				time.Sleep(300 * time.Millisecond) // sched-exempt-ok：限流件
				// 退避节拍（429 实证——逐对连发风暴；sfp 族限速紧，非轮询）
			}
			raw2, err := get429(ctx, okcli, fmt.Sprintf("%s/products?baseCcy=%s&quoteCcy=%s&optType=%s",
				s.EndpointFamily, pr.BaseCcy, pr.QuoteCcy, pr.OptType))
			if err != nil {
				fail++
				if rej != nil {
					rej.Inc("dcd_pair_429")
				}
				continue // T-0180：单对败=跳过续跑（部分集>全集成败丢弃——枚举是覆盖面非事务）
			}
			ps, err := dcd.ParseProducts(raw2)
			if err != nil {
				fail++
				if rej != nil {
					rej.Inc("dcd_pair_429")
				}
				continue // 解析败同型容错（部分集续跑）
			}
			out = append(out, ps...)
		}
		if len(pairs) > 0 && fail*2 > len(pairs) {
			// 半数级失败=系统性断供分型（非零星限频——整面照让位 fail-closed）
			return nil, fmt.Errorf("dcd feed 对级失败率>50%%（%d/%d——系统性断供分型整面让位）", fail, len(pairs))
		}
		return out, nil
	}
}

var httpStatusRe = regexp.MustCompile(`^http (\d{3}):`)

// gapProbeFor 差扫面探针（签名 GET+响应形通用提取：JSON 数组长=products 计数、
// avgRate/annualizedYield 正值=收益带位；错误分型=面缺席〔404/405〕/业务拒
// 〔APIError〕/瞬态〔其余上抛——fail-closed 不裁死〕）。
func gapProbeFor(okcli authGetter) gapscan.Probe {
	return func(ctx context.Context, f gapscan.ProbeFace) (gapscan.ProbeResult, error) {
		if okcli == nil {
			return gapscan.ProbeResult{}, fmt.Errorf("probe 缺席（凭据缺）")
		}
		raw, err := get429(ctx, okcli, f.Endpoint)
		if err != nil {
			var ae *okxexec.APIError
			if errors.As(err, &ae) {
				return gapscan.ProbeResult{}, &gapscan.FaceBizError{Code: ae.Code, Msg: ae.Msg}
			}
			if m := httpStatusRe.FindStringSubmatch(err.Error()); m != nil {
				st, _ := strconv.Atoi(m[1])
				if st == 404 || st == 405 {
					return gapscan.ProbeResult{}, &gapscan.FaceAbsentError{Status: st}
				}
			}
			return gapscan.ProbeResult{}, err
		}
		res := gapscan.ProbeResult{}
		var rows []map[string]any
		if json.Unmarshal(raw, &rows) == nil {
			res.Products = len(rows)
			for _, r := range rows {
				for _, k := range []string{"avgRate", "annualizedYield"} {
					if v, ok := r[k].(string); ok {
						if fv, e2 := strconv.ParseFloat(v, 64); e2 == nil && fv > 0 {
							res.YieldBearing = true
						}
					}
				}
			}
		} else {
			res.Products = 1 // 非数组形（单对象）——面通
		}
		return res, nil
	}
}

// researchArms 研究域四臂发射（T-0117：funding/dcd/差扫/假想官静默告警同挂
// 业主通道——读件缺席=让位 WARN 不炸 boot）。
func researchArms(ctx context.Context, errc chan<- error, pool *pgxpool.Pool,
	cfg *config.Client, okcli authGetter, alerter *notify.Alerter, rej *rejects.Door) {
	fr := funding.NewRunner(pool, cfg)
	fr.AttachAlerter(alerter)
	// T-0150 起草闸读件：SPOT 目录复用 market/tickers 全量面（零新端点）。
	fr.AttachSpotFeed(func(ctx context.Context) ([]byte, error) {
		return get429(ctx, okcli, "/api/v5/market/tickers?instType=SPOT")
	})
	go fr.Run(ctx, errc)
	if en, err := cfg.GetBool("research.dcd.enabled"); err == nil && !en {
		log.Printf("[o-research] dcd 枚举臂退役（research.dcd.enabled=false——D-0037 退场）")
	} else {
		go dcd.NewRunner(pool, cfg).AttachFeed(dcdFeedFor(okcli, rej)).
			AttachCoins(okxCoinsFor(pool, okcli)).AttachAlerter(alerter).Run(ctx, errc)
	}
	go gapscan.NewRunner(pool, cfg).AttachProbe(gapProbeFor(okcli)).AttachAlerter(alerter).Run(ctx, errc)
	hr := hypothesis.NewRunner(pool, cfg)
	hr.AttachAlerter(alerter)
	hr.AttachSignals(hypoSignalReader(pool)) // T-0140 信号读闸接线（sig 域→hypothesis 域投影）
	go hr.Run(ctx, errc)
	go grammar.NewRunner(pool, cfg).AttachCoins(okxCoinsFor(pool, okcli)).Run(ctx, errc) // T-0120 文法枚举臂；T-0215 vol-sell 实评 coins 缝
	// 研究员席静默看门（T-0121：短会话岗无臂拍——DB 双源锚 14 天窗，core 代守）
	sw := researcher.NewSilentWatch(pool)
	sw.AttachAlerter(alerter)
	go sw.Run(ctx)
	// T-0165：backend_xmin 哨兵留 SG（守 PG 非守臂——票面钉死未迁）。
}

// hypoSignalReader 信号读闸出口（T-0140 接线件——sigdist.SurfaceSignals
// 语义映射+Fresh 闸，投影 hypothesis.Sig 窄形：包间类型隔离）。
func hypoSignalReader(pool *pgxpool.Pool) hypothesis.SignalReader {
	adapter := sigdist.SurfaceAdapter(pool)
	return func(ctx context.Context, venue, surface string) ([]hypothesis.Sig, bool, error) {
		ss, ok, err := adapter(ctx, venue, surface)
		if err != nil || !ok {
			return nil, ok, err
		}
		out := make([]hypothesis.Sig, 0, len(ss))
		for _, x := range ss {
			out = append(out, hypothesis.Sig{Kind: x.Kind, Key: x.Key, Val: x.Val, Meta: x.Meta})
		}
		return out, true, nil
	}
}
