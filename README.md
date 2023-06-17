# finish
但愿学生生涯按时finish


## TODO List

### 五月
- [x] 问答数据收集（各医疗网站），用于对llm进行finetune
- [x] 医学知识收集（三元组，儿科教材），NER模型，关系抽取模型，这些直接用现成的，避免无意义造轮子
- [x] 多模态数据收集（图片，视频，音频），先存着
- [x] 知识表征算法学习，例如TransE, TransR, 或者类似R-gcn这种，后续对知识图谱推理可能用到
- [x] 了解收集各种l·lm在24G单卡上finetune的可能性，或者在别人已经微调的llm上继续微调，类似llama-chinese, huatuoGPT

五月主打的就是收集数据

### 六月
- [ ] 知识融合
- [ ] 知识表示
- [ ] 知识驱动的语言预训练模型学习

---

## Reference

1. [python异步下载文件教程（主打全面）](https://blog.51cto.com/lilongsy/6149231)
2. [利用LLM做多模态任务](https://zhuanlan.zhihu.com/p/616351346)
3. [知识注入预训练模型](https://blog.csdn.net/wang2008start/article/details/118371860)
4. [知识表示与融入技术](https://mp.weixin.qq.com/s/necu4iBO6SZG7ImCPMKS4w)
5. [融入知识的预训练语言模型](https://zhuanlan.zhihu.com/p/350649740)


## Paper
### RLHF
1. [RAFT: Reward rAnked FineTuning for Generative Foundation Model Alignment](https://arxiv.org/pdf/2304.06767.pdf)

### Reasoning
1. [ReAct: Synergizing Reasoning and Acting in Language Models](https://arxiv.org/pdf/2210.03629.pdf)

### 知识融合
需要将多个异构图谱融合
