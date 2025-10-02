#!/usr/bin/env python3
"""
srs-box 规则集生成器使用示例

本文件展示了如何使用 srs-box 规则集生成器的基本功能
"""

import sys
from pathlib import Path

# 添加src目录到Python路径
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from src.app import RulesetGenerator
from src.utils.logger import Logger


def example_basic_usage():
    """基本使用示例"""
    print("=== 基本使用示例 ===")

    try:
        # 创建规则集生成器实例
        generator = RulesetGenerator("config.json")

        # 运行完整流程
        success = generator.run()

        if success:
            print("✅ 规则集生成完成")
        else:
            print("❌ 规则集生成失败")

    except Exception as e:
        print(f"❌ 执行异常: {e}")


def example_step_by_step():
    """分步执行示例"""
    print("\n=== 分步执行示例 ===")

    try:
        generator = RulesetGenerator("config.json")

        # 步骤1: 下载阶段
        print("🚀 开始下载阶段...")
        download_success = generator.download_phase()

        if not download_success:
            print("❌ 下载阶段失败")
            return

        # 步骤2: 处理阶段
        print("🔄 开始处理阶段...")
        process_success = generator.process_phase()

        if not process_success:
            print("❌ 处理阶段失败")
            return

        # 步骤3: 编译阶段
        print("⚙️ 开始编译阶段...")
        compile_success = generator.compile_phase()

        if compile_success:
            print("✅ 所有阶段完成")
            generator.show_summary()
        else:
            print("❌ 编译阶段失败")

    except Exception as e:
        print(f"❌ 执行异常: {e}")


def example_custom_config():
    """自定义配置示例"""
    print("\n=== 自定义配置示例 ===")

    # 创建自定义配置文件内容
    custom_config = {
        "rulesets": {
            "test_ip": ["https://core.telegram.org/resources/cidr.txt"],
            "test_json": [
                "https://github.com/Sentsuki/srs-box/raw/refs/heads/main/data/claude.json"
            ],
        },
        "sing_box": {"version": "1.12.8", "platform": "linux-amd64"},
        "version": 3,
    }

    # 保存自定义配置
    import json

    with open("custom_config.json", "w", encoding="utf-8") as f:
        json.dump(custom_config, f, indent=2, ensure_ascii=False)

    try:
        # 使用自定义配置
        generator = RulesetGenerator("custom_config.json")
        success = generator.run()

        if success:
            print("✅ 自定义配置执行成功")
        else:
            print("❌ 自定义配置执行失败")

    except Exception as e:
        print(f"❌ 自定义配置执行异常: {e}")

    # 清理自定义配置文件
    try:
        Path("custom_config.json").unlink()
    except:
        pass


def example_logger_usage():
    """日志使用示例"""
    print("\n=== 日志使用示例 ===")

    logger = Logger()

    # 不同级别的日志输出
    logger.info("这是一条信息日志")
    logger.success("这是一条成功日志")
    logger.warning("这是一条警告日志")
    logger.error("这是一条错误日志")

    # 进度显示
    import time

    for i in range(1, 6):
        logger.progress(i, 5, f"处理第 {i} 个文件")
        time.sleep(0.5)


def main():
    """主函数"""
    print("🌏 srs-box 规则集生成器使用示例")
    print("=" * 60)

    # 基本使用示例
    example_basic_usage()

    # 分步执行示例
    example_step_by_step()

    # 自定义配置示例
    example_custom_config()

    # 日志使用示例
    example_logger_usage()

    print("\n" + "=" * 60)
    print("📚 更多信息请查看 README.md 文档")


if __name__ == "__main__":
    main()
