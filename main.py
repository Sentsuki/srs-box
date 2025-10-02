#!/usr/bin/env python3

"""
srs-box 规则集生成器主程序
使用重构后的应用架构，提供统一的错误处理和日志输出
"""

# 添加src目录到Python路径
import sys
from pathlib import Path

src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from src.app import RulesetGenerator


def main():
    """主函数"""
    try:
        # 创建规则集生成器实例
        generator = RulesetGenerator()

        # 运行完整流程
        success = generator.run()

        # 根据执行结果设置退出码
        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        print("\n⚠️ 用户中断执行")
        sys.exit(130)  # 标准的键盘中断退出码

    except Exception as e:
        print(f"❌ 程序执行异常: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
