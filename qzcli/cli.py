#!/usr/bin/env python3
"""
qzcli - 启智平台任务管理 CLI
"""

import sys
import time
import argparse
from pathlib import Path
from typing import Optional, List

from . import __version__
from .config import (
    init_config, get_credentials, load_config, CONFIG_DIR, 
    save_cookie, get_cookie, clear_cookie,
    save_resources, get_workspace_resources, load_all_resources,
    set_workspace_name, find_workspace_by_name, find_resource_by_name,
    list_cached_workspaces,
)
from .api import get_api, QzAPIError
from .store import get_store, JobRecord
from .display import get_display, format_duration, format_time_ago


def cmd_init(args):
    """初始化配置"""
    display = get_display()
    
    username = args.username
    password = args.password
    
    if not username:
        username = input("请输入启智平台用户名: ").strip()
    if not password:
        import getpass
        password = getpass.getpass("请输入密码: ").strip()
    
    if not username or not password:
        display.print_error("用户名和密码不能为空")
        return 1
    
    init_config(username, password)
    
    # 测试连接
    display.print("正在验证连接...")
    api = get_api()
    if api.test_connection():
        display.print_success("配置成功！认证信息已保存")
        display.print(f"配置目录: {CONFIG_DIR}")
        return 0
    else:
        display.print_error("认证失败，请检查用户名和密码")
        return 1


def cmd_list_cookie(args):
    """使用 cookie 从 API 获取任务列表"""
    display = get_display()
    api = get_api()
    
    # 获取 cookie
    cookie_data = get_cookie()
    if not cookie_data or not cookie_data.get("cookie"):
        display.print_error("未设置 cookie，请先运行: qzcli cookie -f cookies.txt")
        return 1
    
    cookie = cookie_data["cookie"]
    
    # 确定要查询的工作空间列表
    workspace_input = args.workspace
    
    if args.all_ws:
        # 查询所有已缓存的工作空间
        all_resources = load_all_resources()
        if not all_resources:
            display.print_error("没有已缓存的工作空间")
            display.print("[dim]请先运行: qzcli res -w <workspace_id> -u[/dim]")
            return 1
        workspace_ids = [(ws_id, data.get("name", "")) for ws_id, data in all_resources.items()]
    elif workspace_input:
        # 指定的工作空间
        if workspace_input.startswith("ws-"):
            workspace_id = workspace_input
            ws_resources = get_workspace_resources(workspace_id)
            ws_name = ws_resources.get("name", "") if ws_resources else ""
        else:
            workspace_id = find_workspace_by_name(workspace_input)
            if workspace_id:
                ws_resources = get_workspace_resources(workspace_id)
                ws_name = ws_resources.get("name", "") if ws_resources else workspace_input
            else:
                display.print_error(f"未找到名称为 '{workspace_input}' 的工作空间")
                display.print("[dim]使用 qzcli res --list 查看已缓存的工作空间[/dim]")
                return 1
        workspace_ids = [(workspace_id, ws_name)]
    else:
        # 使用默认工作空间
        default_ws = cookie_data.get("workspace_id", "")
        if not default_ws:
            display.print_error("请指定工作空间: qzcli ls -c -w <名称或ID>")
            display.print("[dim]或使用 --all-ws 查询所有已缓存的工作空间[/dim]")
            return 1
        ws_resources = get_workspace_resources(default_ws)
        ws_name = ws_resources.get("name", "") if ws_resources else ""
        workspace_ids = [(default_ws, ws_name)]
    
    all_jobs = []
    
    for workspace_id, ws_name in workspace_ids:
        try:
            if len(workspace_ids) > 1:
                display.print(f"[dim]正在获取 {ws_name or workspace_id} 的任务...[/dim]")
            else:
                display.print(f"[dim]正在从 API 获取任务列表...[/dim]")
            
            result = api.list_jobs_with_cookie(
                workspace_id, 
                cookie, 
                page_size=args.limit * 2 if args.running else args.limit
            )
            
            jobs_data = result.get("jobs", [])
            
            # 转换为 JobRecord 格式
            for job_data in jobs_data:
                job = JobRecord.from_api_response(job_data, source="api_cookie")
                # 添加工作空间名称
                if ws_name:
                    job.metadata["workspace_name"] = ws_name
                all_jobs.append(job)
                
        except QzAPIError as e:
            if "401" in str(e) or "过期" in str(e):
                display.print_error("Cookie 已过期，请重新设置: qzcli cookie -f <cookie_file>")
                return 1
            display.print_warning(f"获取 {ws_name or workspace_id} 失败: {e}")
            continue
    
    if not all_jobs:
        display.print("[dim]暂无任务[/dim]")
        return 0
    
    # 按创建时间排序
    all_jobs.sort(key=lambda x: x.created_at or "", reverse=True)
    
    # 过滤状态
    if args.status:
        all_jobs = [j for j in all_jobs if args.status.lower() in j.status.lower()]
    
    # 过滤运行中的任务
    if args.running:
        active_statuses = {"job_running", "job_queuing", "job_pending", "running", "queuing", "pending"}
        all_jobs = [
            j for j in all_jobs
            if j.status.lower() in active_statuses or "running" in j.status.lower() or "queue" in j.status.lower()
        ]
    
    # 限制数量
    all_jobs = all_jobs[:args.limit]
    
    if not all_jobs:
        display.print("[dim]暂无符合条件的任务[/dim]")
        return 0
    
    # 显示标题
    if len(workspace_ids) == 1:
        ws_name = workspace_ids[0][1]
        if ws_name:
            display.print(f"\n[bold]工作空间: {ws_name}[/bold]\n")
    
    # 复用现有显示函数
    if args.wide and not args.compact:
        display.print_jobs_wide(all_jobs)
    else:
        display.print_jobs_table(all_jobs, show_command=args.verbose, show_url=args.url)
    
    return 0


def cmd_list(args):
    """列出任务"""
    # Cookie 模式：从 API 获取任务
    if args.cookie:
        return cmd_list_cookie(args)
    
    display = get_display()
    store = get_store()
    api = get_api()
    
    # 获取本地存储的任务
    # 如果使用 --running，先获取更多任务再过滤
    fetch_limit = args.limit * 3 if args.running else args.limit
    jobs = store.list(limit=fetch_limit, status=args.status)
    
    if not jobs:
        display.print("[dim]暂无任务记录，使用 qzcli import 导入或 qzcli track 添加任务[/dim]")
        return 0
    
    # 更新任务状态
    if not args.no_refresh:
        display.print("[dim]正在更新任务状态...[/dim]")
        
        # 只更新非终态任务
        job_ids_to_update = [
            j.job_id for j in jobs
            if j.status not in ("job_succeeded", "job_failed", "job_stopped")
        ]
        
        if job_ids_to_update:
            try:
                results = api.get_jobs_detail(job_ids_to_update)
                for job_id, data in results.items():
                    if "error" not in data:
                        store.update_from_api(job_id, data)
            except QzAPIError as e:
                display.print_warning(f"部分任务状态更新失败: {e}")
        
        # 重新获取更新后的列表
        jobs = store.list(limit=fetch_limit, status=args.status)
    
    # 过滤：只显示运行中/排队中的任务
    if args.running:
        active_statuses = {"job_running", "job_queuing", "job_pending", "running", "queuing", "pending"}
        jobs = [
            j for j in jobs
            if j.status.lower() in active_statuses or "running" in j.status.lower() or "queue" in j.status.lower()
        ]
        # 应用 limit
        jobs = jobs[:args.limit]
        
        if not jobs:
            display.print("[dim]暂无运行中的任务[/dim]")
            return 0
    
    if args.wide and not args.compact:
        display.print_jobs_wide(jobs)
    else:
        display.print_jobs_table(jobs, show_command=args.verbose, show_url=args.url)
    return 0


def cmd_status(args):
    """查看任务状态"""
    display = get_display()
    store = get_store()
    api = get_api()
    
    job_id = args.job_id
    
    # 从 API 获取最新状态
    try:
        api_data = api.get_job_detail(job_id)
        job = store.update_from_api(job_id, api_data)
        display.print_job_detail(job, api_data)
        
        if args.json:
            import json
            print(json.dumps(api_data, indent=2, ensure_ascii=False))
        
        return 0
    except QzAPIError as e:
        display.print_error(f"查询失败: {e}")
        return 1


def cmd_stop(args):
    """停止任务"""
    display = get_display()
    store = get_store()
    api = get_api()
    
    job_id = args.job_id
    
    # 确认
    if not args.yes:
        confirm = input(f"确定要停止任务 {job_id}? [y/N] ").strip().lower()
        if confirm != "y":
            display.print("已取消")
            return 0
    
    try:
        if api.stop_job(job_id):
            display.print_success(f"任务 {job_id} 已停止")
            # 更新本地状态
            store.update(job_id, status="job_stopped")
            return 0
        else:
            display.print_error("停止任务失败")
            return 1
    except QzAPIError as e:
        display.print_error(f"停止任务失败: {e}")
        return 1


def cmd_watch(args):
    """实时监控任务状态"""
    display = get_display()
    store = get_store()
    api = get_api()
    
    interval = args.interval
    
    display.print(f"[bold]实时监控模式[/bold] (每 {interval} 秒刷新，按 Ctrl+C 退出)")
    display.print("")
    
    try:
        while True:
            # 获取所有非终态任务
            jobs = store.list()
            active_jobs = [
                j for j in jobs
                if j.status not in ("job_succeeded", "job_failed", "job_stopped")
            ]
            
            # 更新状态
            if active_jobs:
                job_ids = [j.job_id for j in active_jobs]
                try:
                    results = api.get_jobs_detail(job_ids)
                    for job_id, data in results.items():
                        if "error" not in data:
                            store.update_from_api(job_id, data)
                except QzAPIError:
                    pass
            
            # 清屏并显示
            print("\033[2J\033[H", end="")  # 清屏
            
            jobs = store.list(limit=args.limit)
            display.print_jobs_table(
                jobs,
                title=f"启智平台任务监控 (每 {interval}s 刷新)"
            )
            
            # 检查是否还有活跃任务
            active_count = sum(
                1 for j in jobs
                if j.status not in ("job_succeeded", "job_failed", "job_stopped")
            )
            
            if active_count == 0 and not args.keep_alive:
                display.print("\n[green]所有任务已完成[/green]")
                break
            
            time.sleep(interval)
    
    except KeyboardInterrupt:
        display.print("\n[dim]监控已停止[/dim]")
    
    return 0


def cmd_track(args):
    """追踪任务（供脚本调用）"""
    display = get_display()
    store = get_store()
    api = get_api()
    
    job_id = args.job_id
    
    # 尝试从 API 获取详情
    try:
        api_data = api.get_job_detail(job_id)
        job = JobRecord.from_api_response(api_data, source=args.source or "")
    except QzAPIError:
        # API 失败时创建最小记录
        job = JobRecord(
            job_id=job_id,
            name=args.name or "",
            source=args.source or "",
            workspace_id=args.workspace or "",
        )
    
    # 更新元数据
    if args.name:
        job.name = args.name
    if args.source:
        job.source = args.source
    if args.workspace:
        job.workspace_id = args.workspace
    
    store.add(job)
    
    if not args.quiet:
        display.print_success(f"已追踪任务: {job_id}")
    
    return 0


def cmd_import(args):
    """从文件导入任务"""
    display = get_display()
    store = get_store()
    api = get_api()
    
    filepath = Path(args.file)
    if not filepath.exists():
        display.print_error(f"文件不存在: {filepath}")
        return 1
    
    count = store.import_from_file(filepath, source=args.source or filepath.name)
    display.print_success(f"已导入 {count} 个任务")
    
    # 可选：更新导入任务的状态
    if args.refresh and count > 0:
        display.print("正在更新任务状态...")
        jobs = store.list()
        job_ids = [j.job_id for j in jobs if not j.status or j.status == "unknown"]
        
        if job_ids:
            try:
                results = api.get_jobs_detail(job_ids[:50])  # 最多更新 50 个
                updated = 0
                for job_id, data in results.items():
                    if "error" not in data:
                        store.update_from_api(job_id, data)
                        updated += 1
                display.print_success(f"已更新 {updated} 个任务状态")
            except QzAPIError as e:
                display.print_warning(f"状态更新失败: {e}")
    
    return 0


def cmd_remove(args):
    """删除任务记录"""
    display = get_display()
    store = get_store()
    
    job_id = args.job_id
    
    if not args.yes:
        confirm = input(f"确定要删除任务记录 {job_id}? [y/N] ").strip().lower()
        if confirm != "y":
            display.print("已取消")
            return 0
    
    if store.remove(job_id):
        display.print_success(f"已删除任务记录: {job_id}")
        return 0
    else:
        display.print_error(f"任务不存在: {job_id}")
        return 1


def cmd_clear(args):
    """清空所有任务记录"""
    display = get_display()
    store = get_store()
    
    count = store.count()
    
    if count == 0:
        display.print("暂无任务记录")
        return 0
    
    if not args.yes:
        confirm = input(f"确定要清空所有 {count} 个任务记录? [y/N] ").strip().lower()
        if confirm != "y":
            display.print("已取消")
            return 0
    
    store.clear()
    display.print_success(f"已清空 {count} 个任务记录")
    return 0


def cmd_cookie(args):
    """设置浏览器 cookie"""
    display = get_display()
    
    if args.clear:
        clear_cookie()
        display.print_success("已清除 cookie")
        return 0
    
    if args.show:
        cookie_data = get_cookie()
        if cookie_data:
            display.print(f"Workspace: {cookie_data.get('workspace_id', 'N/A')}")
            display.print(f"Cookie: {cookie_data.get('cookie', '')[:80]}...")
        else:
            display.print("[dim]未设置 cookie[/dim]")
        return 0
    
    cookie = args.cookie
    workspace_id = args.workspace or ""
    
    # 支持从文件读取 cookie
    if args.file:
        filepath = Path(args.file)
        if not filepath.exists():
            display.print_error(f"文件不存在: {filepath}")
            return 1
        with open(filepath, "r") as f:
            lines = f.readlines()
            # 取最后一个非空行作为 cookie
            for line in reversed(lines):
                line = line.strip()
                if line and not line.startswith("#") and line != "cookie":
                    cookie = line
                    break
        if not cookie:
            display.print_error("文件中未找到有效的 cookie")
            return 1
        display.print(f"[dim]从文件读取 cookie: {filepath}[/dim]")
    
    if not cookie:
        display.print("请输入浏览器 cookie（从 F12 Network 中复制）:")
        display.print("[dim]提示: 在 qz.sii.edu.cn 页面按 F12 -> Console -> 输入 document.cookie[/dim]")
        cookie = input().strip()
    
    if not cookie:
        display.print_error("cookie 不能为空")
        return 1
    
    # 测试 cookie 是否有效（使用 /openapi/v1/train_job/list 端点）
    if not args.no_test and workspace_id:
        display.print("正在验证 cookie...")
        api = get_api()
        try:
            result = api.list_jobs_with_cookie(workspace_id, cookie, page_size=1)
            total = result.get("total", 0)
            display.print_success(f"Cookie 有效！工作空间内有 {total} 个任务")
        except QzAPIError as e:
            display.print_error(f"Cookie 无效: {e}")
            return 1
    
    save_cookie(cookie, workspace_id)
    display.print_success("Cookie 已保存")
    return 0


def cmd_workspaces(args):
    """从历史任务中提取工作空间和资源配置（支持本地缓存）"""
    display = get_display()
    api = get_api()
    
    # 如果是列出所有已缓存的工作空间
    if args.list:
        cached = list_cached_workspaces()
        if not cached:
            display.print("[dim]暂无已缓存的工作空间，使用 qzcli res -w <workspace_id> 添加[/dim]")
            return 0
        
        display.print(f"\n[bold]已缓存的工作空间 ({len(cached)} 个)[/bold]\n")
        for ws in cached:
            name = ws.get("name") or "[未命名]"
            import datetime
            updated = datetime.datetime.fromtimestamp(ws.get("updated_at", 0)).strftime("%Y-%m-%d %H:%M")
            display.print(f"  [bold]{name}[/bold]")
            display.print(f"    ID: [cyan]{ws['id']}[/cyan]")
            display.print(f"    资源: {ws['project_count']} 项目, {ws['compute_group_count']} 计算组, {ws['spec_count']} 规格")
            display.print(f"    更新: {updated}")
            display.print("")
        
        display.print("[dim]使用方法:[/dim]")
        display.print("  qzcli res -w <名称或ID>      # 查看资源")
        display.print("  qzcli res -w <ID> -u         # 更新缓存")
        display.print("  qzcli res -w <ID> --name 别名  # 设置名称")
        return 0
    
    # 如果只设置名称（没有 -u 参数）
    if hasattr(args, 'name') and args.name and not args.update:
        workspace_id = args.workspace
        if not workspace_id:
            display.print_error("请指定工作空间 ID: qzcli res -w <workspace_id> --name <名称>")
            return 1
        set_workspace_name(workspace_id, args.name)
        display.print_success(f"已设置工作空间名称: {args.name}")
        return 0
    
    # 记录要设置的名称（如果有）
    pending_name = args.name if hasattr(args, 'name') else None
    
    # 解析 workspace 参数（支持名称或 ID）
    workspace_input = args.workspace
    cookie_data = get_cookie()
    
    if not workspace_input:
        workspace_id = cookie_data.get("workspace_id", "") if cookie_data else ""
    elif workspace_input.startswith("ws-"):
        workspace_id = workspace_input
    else:
        # 尝试通过名称查找
        workspace_id = find_workspace_by_name(workspace_input)
        if workspace_id:
            display.print(f"[dim]匹配到工作空间: {workspace_input} -> {workspace_id}[/dim]")
        else:
            display.print_error(f"未找到名称为 '{workspace_input}' 的工作空间")
            display.print("[dim]使用 qzcli res --list 查看已缓存的工作空间[/dim]")
            return 1
    
    if not workspace_id:
        display.print_error("请指定工作空间: qzcli res -w <名称或ID>")
        display.print("[dim]使用 qzcli res --list 查看已缓存的工作空间[/dim]")
        return 1
    
    # 检查是否需要从 API 更新
    cached_resources = get_workspace_resources(workspace_id)
    use_cache = cached_resources and not args.update
    
    if use_cache:
        # 使用缓存
        import datetime
        updated = datetime.datetime.fromtimestamp(cached_resources.get("updated_at", 0)).strftime("%Y-%m-%d %H:%M")
        ws_name = cached_resources.get("name", "")
        title = f"资源配置"
        if ws_name:
            title += f" [{ws_name}]"
        title += f" (缓存于 {updated})"
        
        display.print(f"\n[bold]{title}[/bold]")
        display.print(f"[dim]工作空间: {workspace_id}[/dim]\n")
        
        # 转换缓存格式为列表格式
        projects = list(cached_resources.get("projects", {}).values())
        compute_groups = list(cached_resources.get("compute_groups", {}).values())
        specs = list(cached_resources.get("specs", {}).values())
    else:
        # 从 API 获取
        if not cookie_data or not cookie_data.get("cookie"):
            display.print_error("未设置 cookie，请先运行: qzcli cookie -f cookies.txt")
            display.print("[dim]提示: 从浏览器 F12 获取 cookie[/dim]")
            return 1
        
        cookie = cookie_data["cookie"]
        
        try:
            display.print("[dim]正在从历史任务中提取资源配置...[/dim]")
            
            # 获取任务列表
            result = api.list_jobs_with_cookie(workspace_id, cookie, page_size=200)
            jobs = result.get("jobs", [])
            total = result.get("total", 0)
            
            if not jobs:
                display.print("未找到任务记录")
                return 0
            
            # 提取资源信息
            resources = api.extract_resources_from_jobs(jobs)
            
            # 保存到本地缓存
            ws_name = pending_name or (cached_resources.get("name", "") if cached_resources else "")
            save_resources(workspace_id, resources, ws_name)
            display.print_success("资源配置已保存到本地缓存")
            
            display.print(f"\n[bold]资源配置（从 {len(jobs)}/{total} 个任务中提取）[/bold]")
            display.print(f"[dim]工作空间: {workspace_id}[/dim]\n")
            
            projects = resources.get("projects", [])
            compute_groups = resources.get("compute_groups", [])
            specs = resources.get("specs", [])
            
        except QzAPIError as e:
            if "401" in str(e) or "过期" in str(e):
                display.print_error("Cookie 已过期，请重新设置: qzcli cookie -f <cookie_file>")
            else:
                display.print_error(f"获取失败: {e}")
            return 1
    
    # 显示项目
    if projects:
        display.print(f"[bold]项目 ({len(projects)} 个)[/bold]")
        for proj in projects:
            display.print(f"  - {proj['name']}")
            display.print(f"    [cyan]{proj['id']}[/cyan]")
        display.print("")
    
    # 显示计算组
    if compute_groups:
        display.print(f"[bold]计算组 ({len(compute_groups)} 个)[/bold]")
        for group in compute_groups:
            gpu_type = group.get("gpu_type", "")
            gpu_display = group.get("gpu_type_display", "")
            display.print(f"  - {group['name']} [{gpu_type}]")
            if gpu_display:
                display.print(f"    [dim]{gpu_display}[/dim]")
            display.print(f"    [cyan]{group['id']}[/cyan]")
        display.print("")
    
    # 显示规格
    if specs:
        display.print(f"[bold]GPU 规格 ({len(specs)} 个)[/bold]")
        for spec in specs:
            gpu_type = spec.get("gpu_type", "")
            gpu_count = spec.get("gpu_count", 0)
            cpu_count = spec.get("cpu_count", 0)
            mem_gb = spec.get("memory_gb", 0)
            display.print(f"  - {gpu_count}x {gpu_type} + {cpu_count}核CPU + {mem_gb}GB内存")
            display.print(f"    [cyan]{spec['id']}[/cyan]")
        display.print("")
    
    # 导出格式
    if args.export:
        display.print("[bold]导出格式（可用于 shell 脚本）:[/bold]")
        display.print(f'WORKSPACE_ID="{workspace_id}"')
        if projects:
            display.print(f'PROJECT_ID="{projects[0]["id"]}"  # {projects[0]["name"]}')
        if compute_groups:
            for group in compute_groups:
                display.print(f'# {group["name"]} [{group.get("gpu_type", "")}]')
                display.print(f'LOGIC_COMPUTE_GROUP_ID="{group["id"]}"')
        if specs:
            for spec in specs:
                display.print(f'# {spec.get("gpu_count", 0)}x {spec.get("gpu_type", "")}')
                display.print(f'SPEC_ID="{spec["id"]}"')
    
    return 0


def cmd_resources(args):
    """列出工作空间内可用的计算资源（cmd_workspaces 的别名）"""
    # 直接调用 workspaces 命令
    return cmd_workspaces(args)


def cmd_avail(args):
    """查询计算组空余节点，帮助决定任务应该提交到哪里"""
    display = get_display()
    api = get_api()
    
    # 获取 cookie
    cookie_data = get_cookie()
    if not cookie_data or not cookie_data.get("cookie"):
        display.print_error("未设置 cookie，请先运行: qzcli cookie -f cookies.txt")
        return 1
    
    cookie = cookie_data["cookie"]
    
    # 解析 workspace 参数（支持名称或 ID）
    workspace_input = args.workspace
    
    # 如果不指定 workspace，查询所有已缓存的工作空间
    if not workspace_input:
        all_resources = load_all_resources()
        if not all_resources:
            display.print_error("没有已缓存的工作空间")
            display.print("[dim]请先运行: qzcli res -w <workspace_id> -u[/dim]")
            return 1
        workspace_ids = list(all_resources.keys())
    elif workspace_input.startswith("ws-"):
        workspace_ids = [workspace_input]
    else:
        workspace_id = find_workspace_by_name(workspace_input)
        if workspace_id:
            workspace_ids = [workspace_id]
            display.print(f"[dim]匹配到工作空间: {workspace_input} -> {workspace_id}[/dim]")
        else:
            display.print_error(f"未找到名称为 '{workspace_input}' 的工作空间")
            display.print("[dim]使用 qzcli res --list 查看已缓存的工作空间[/dim]")
            return 1
    
    required_nodes = args.nodes
    group_filter = args.group
    all_results = []  # 所有工作空间的结果汇总
    
    for workspace_id in workspace_ids:
        # 获取计算组列表（从缓存）
        cached_resources = get_workspace_resources(workspace_id)
        if not cached_resources:
            display.print_warning(f"未缓存工作空间 {workspace_id} 的资源信息，跳过")
            continue
        
        compute_groups = cached_resources.get("compute_groups", {})
        specs = cached_resources.get("specs", {})
        ws_name = cached_resources.get("name", "") or workspace_id
        
        # 如果指定了特定计算组
        if group_filter:
            if group_filter.startswith("lcg-"):
                if group_filter in compute_groups:
                    compute_groups = {group_filter: compute_groups[group_filter]}
                else:
                    continue  # 该工作空间没有这个计算组
            else:
                found = find_resource_by_name(workspace_id, "compute_groups", group_filter)
                if found:
                    compute_groups = {found["id"]: found}
                else:
                    continue
        
        if not compute_groups:
            continue
        
        display.print(f"[dim]正在查询 {ws_name} 的 {len(compute_groups)} 个计算组...[/dim]")
        
        try:
            for lcg_id, lcg_info in compute_groups.items():
                lcg_name = lcg_info.get("name", lcg_id)
                gpu_type = lcg_info.get("gpu_type", "")
                
                try:
                    data = api.list_node_dimension(workspace_id, cookie, lcg_id, page_size=200)
                    nodes = data.get("node_dimensions", [])
                    total_nodes = len(nodes)
                    
                    # 统计空闲节点（GPU 使用数为 0）
                    free_nodes = []
                    for node in nodes:
                        gpu_info = node.get("gpu", {})
                        gpu_used = gpu_info.get("used", 0)
                        gpu_total = gpu_info.get("total", 0)
                        if gpu_used == 0 and gpu_total > 0:
                            free_nodes.append({
                                "name": node.get("name", ""),
                                "gpu_total": gpu_total,
                            })
                    
                    all_results.append({
                        "workspace_id": workspace_id,
                        "workspace_name": ws_name,
                        "id": lcg_id,
                        "name": lcg_name,
                        "gpu_type": gpu_type,
                        "total_nodes": total_nodes,
                        "free_nodes": len(free_nodes),
                        "free_node_list": free_nodes,
                        "specs": specs,
                    })
                except QzAPIError as e:
                    display.print_warning(f"查询 {lcg_name} 失败: {e}")
                    continue
        except QzAPIError as e:
            if "401" in str(e) or "过期" in str(e):
                display.print_error("Cookie 已过期，请重新设置: qzcli cookie -f <cookie_file>")
                return 1
            display.print_warning(f"查询 {ws_name} 失败: {e}")
            continue
    
    if not all_results:
        display.print_error("未能获取任何计算组的节点信息")
        return 1
    
    display.print(f"\n[bold]空余节点汇总[/bold]\n")
    
    # 如果指定了节点需求，过滤并推荐
    if required_nodes:
        # 按空闲节点数降序排序（跨工作空间）
        all_results.sort(key=lambda x: x["free_nodes"], reverse=True)
        available = [r for r in all_results if r["free_nodes"] >= required_nodes]
        
        if not available:
            display.print(f"[red]没有计算组有 >= {required_nodes} 个空闲节点[/red]\n")
            display.print("当前各计算组空闲节点数：")
            for r in all_results:
                display.print(f"  [{r['workspace_name']}] {r['name']}: {r['free_nodes']} 空节点 [{r['gpu_type']}]")
            return 1
        
        display.print(f"需要 {required_nodes} 个节点，以下计算组可用：\n")
        
        for r in available:
            display.print(f"[green]✓[/green] [{r['workspace_name']}] [bold]{r['name']}[/bold]  {r['free_nodes']} 空节点 [{r['gpu_type']}]")
            display.print(f"  [cyan]{r['id']}[/cyan]")
            # 显示空闲节点列表
            if args.verbose and r.get('free_node_list'):
                node_names = [n['name'] for n in r['free_node_list']]
                display.print(f"  [dim]空闲节点: {', '.join(node_names)}[/dim]")
        
        # 导出格式
        if args.export:
            display.print("")
            best = available[0]
            display.print(f"# 推荐: [{best['workspace_name']}] {best['name']} ({best['free_nodes']} 空节点)")
            display.print(f'WORKSPACE_ID="{best["workspace_id"]}"')
            display.print(f'LOGIC_COMPUTE_GROUP_ID="{best["id"]}"')
            specs = best.get("specs", {})
            if specs:
                spec = list(specs.values())[0]
                display.print(f'SPEC_ID="{spec["id"]}"  # {spec.get("gpu_count", 0)}x {spec.get("gpu_type", "")}')
    else:
        # 按工作空间分组，组内按空闲节点数降序
        from collections import defaultdict
        by_workspace = defaultdict(list)
        for r in all_results:
            by_workspace[r['workspace_name']].append(r)
        
        for ws_name, results in by_workspace.items():
            results.sort(key=lambda x: x["free_nodes"], reverse=True)
            display.print(f"[bold]{ws_name}[/bold]")
            display.print(f"{'  计算组':<27} {'空节点':>6} {'总节点':>6} {'GPU类型':<10}")
            display.print("  " + "-" * 53)
            for r in results:
                name_display = r['name'][:23] if len(r['name']) > 23 else r['name']
                display.print(f"  {name_display:<25} {r['free_nodes']:>6} {r['total_nodes']:>6} {r['gpu_type']:<10}")
                # 显示空闲节点列表
                if args.verbose and r.get('free_node_list'):
                    node_names = [n['name'] for n in r['free_node_list']]
                    display.print(f"    [dim]空闲: {', '.join(node_names)}[/dim]")
            display.print("")
        
        # 导出格式
        if args.export:
            display.print("[bold]导出格式:[/bold]")
            for r in sorted(all_results, key=lambda x: x["free_nodes"], reverse=True):
                if r['free_nodes'] > 0:
                    display.print(f"# [{r['workspace_name']}] {r['name']} ({r['free_nodes']} 空节点)")
                    display.print(f'WORKSPACE_ID="{r["workspace_id"]}"')
                    display.print(f'LOGIC_COMPUTE_GROUP_ID="{r["id"]}"')
    
    return 0


def cmd_workspace(args):
    """查看工作空间内所有运行任务"""
    display = get_display()
    api = get_api()
    
    # 获取 cookie
    cookie_data = get_cookie()
    if not cookie_data or not cookie_data.get("cookie"):
        display.print_error("未设置 cookie，请先运行: qzcli cookie -f cookies.txt")
        display.print("[dim]提示: 从浏览器 F12 获取 cookie[/dim]")
        return 1
    
    cookie = cookie_data["cookie"]
    workspace_id = args.workspace or cookie_data.get("workspace_id", "")
    
    # 如果没有指定 workspace，列出可用的 workspace 供选择
    if not workspace_id:
        display.print("[yellow]未设置默认工作空间，正在获取可用列表...[/yellow]\n")
        try:
            workspaces = api.list_workspaces(cookie)
            if workspaces:
                display.print("[bold]请选择一个工作空间:[/bold]\n")
                for idx, ws in enumerate(workspaces, 1):
                    ws_id = ws.get("id", "")
                    ws_name = ws.get("name", "未命名")
                    display.print(f"  [{idx}] {ws_name}")
                    display.print(f"      [dim]{ws_id}[/dim]")
                display.print("")
                display.print("[dim]使用方法:[/dim]")
                display.print("  qzcli ws -w <workspace_id>")
                display.print("  qzcli cookie -w <workspace_id>  # 设置默认")
            else:
                display.print_error("未找到可访问的工作空间")
        except QzAPIError as e:
            display.print_error(f"获取工作空间列表失败: {e}")
        return 1
    
    # 项目过滤
    project_filter = None if args.all else args.project
    
    try:
        display.print("[dim]正在获取工作空间任务...[/dim]")
        result = api.list_workspace_tasks(
            workspace_id, 
            cookie,
            page_num=args.page,
            page_size=args.size,
            project_filter=project_filter,
        )
        
        tasks = result.get("task_dimensions", [])
        total = result.get("total", 0)
        
        if not tasks:
            if project_filter:
                display.print(f"[dim]项目 '{project_filter}' 暂无运行中的任务[/dim]")
            else:
                display.print("工作空间内暂无运行中的任务")
            return 0
        
        # 统计 GPU 使用
        total_gpu = sum(t.get("gpu", {}).get("total", 0) for t in tasks)
        avg_gpu_usage = sum(t.get("gpu", {}).get("usage_rate", 0) for t in tasks) / len(tasks) * 100 if tasks else 0
        
        title = f"工作空间任务概览"
        if project_filter:
            title += f" [{project_filter}]"
        title += f" (显示 {len(tasks)}/{total} 个, {total_gpu} GPU, 平均利用率 {avg_gpu_usage:.1f}%)"
        
        display.print(f"\n[bold]{title}[/bold]\n")
        
        # 同步到本地任务列表
        synced_count = 0
        if args.sync:
            store = get_store()
            for task in tasks:
                job_id = task.get("id", "")
                if job_id and not store.get_job(job_id):
                    # 创建简化的 JobRecord
                    from .store import JobRecord
                    job = JobRecord(
                        job_id=job_id,
                        name=task.get("name", ""),
                        status=task.get("status", "UNKNOWN").lower(),
                        source="workspace_sync",
                        workspace_id=workspace_id,
                        project_name=task.get("project", {}).get("name", ""),
                    )
                    store.add_job(job)
                    synced_count += 1
            if synced_count > 0:
                display.print_success(f"已同步 {synced_count} 个新任务到本地")
        
        for idx, task in enumerate(tasks, 1):
            name = task.get("name", "")
            status = task.get("status", "UNKNOWN")
            gpu_total = task.get("gpu", {}).get("total", 0)
            gpu_usage = task.get("gpu", {}).get("usage_rate", 0) * 100
            cpu_usage = task.get("cpu", {}).get("usage_rate", 0) * 100
            mem_usage = task.get("memory", {}).get("usage_rate", 0) * 100
            nodes_info = task.get("nodes_occupied", {})
            nodes_count = nodes_info.get("count", 0)
            nodes_list = nodes_info.get("nodes", [])
            user_name = task.get("user", {}).get("name", "")
            project_name = task.get("project", {}).get("name", "")
            running_time = format_duration(task.get("running_time_ms", ""))
            job_id = task.get("id", "")
            
            # 状态颜色
            if status == "RUNNING":
                status_icon = "[cyan]●[/cyan]"
            elif status == "QUEUING":
                status_icon = "[yellow]◌[/yellow]"
            else:
                status_icon = "[dim]?[/dim]"
            
            # GPU 使用率颜色
            if gpu_usage >= 80:
                gpu_color = "green"
            elif gpu_usage >= 50:
                gpu_color = "yellow"
            else:
                gpu_color = "red"
            
            display.print(f"[bold][{idx:2d}][/bold] {status_icon} {name}")
            display.print(f"     [{gpu_color}]{gpu_total} GPU ({gpu_usage:.0f}%)[/{gpu_color}] | CPU {cpu_usage:.0f}% | MEM {mem_usage:.0f}% | {running_time} | {user_name}")
            display.print(f"     [dim]{project_name} | {nodes_count} 节点: {', '.join(nodes_list[:3])}{'...' if len(nodes_list) > 3 else ''}[/dim]")
            display.print(f"     [dim]{job_id}[/dim]")
            display.print("")
        
        return 0
        
    except QzAPIError as e:
        if "401" in str(e) or "过期" in str(e):
            display.print_error("Cookie 已过期，请重新设置: qzcli cookie -f <cookie_file> -w <workspace_id>")
        else:
            display.print_error(f"获取失败: {e}")
        return 1


def cmd_login(args):
    """通过 CAS 登录获取 cookie"""
    import getpass
    
    display = get_display()
    api = get_api()
    
    # 获取用户名
    username = args.username
    if not username:
        try:
            username = input("学工号: ").strip()
        except (EOFError, KeyboardInterrupt):
            display.print("\n[dim]已取消[/dim]")
            return 1
    
    if not username:
        display.print_error("用户名不能为空")
        return 1
    
    # 获取密码
    password = args.password
    if not password:
        try:
            password = getpass.getpass("密码: ")
        except (EOFError, KeyboardInterrupt):
            display.print("\n[dim]已取消[/dim]")
            return 1
    
    if not password:
        display.print_error("密码不能为空")
        return 1
    
    display.print("[dim]正在登录...[/dim]")
    
    try:
        cookie = api.login_with_cas(username, password)
        
        # 保存 cookie
        save_cookie(cookie, workspace_id=args.workspace)
        
        display.print_success("登录成功！Cookie 已保存")
        
        # 显示 cookie 前几个字符
        cookie_preview = cookie[:50] + "..." if len(cookie) > 50 else cookie
        display.print(f"[dim]Cookie: {cookie_preview}[/dim]")
        
        if args.workspace:
            display.print(f"[dim]默认工作空间: {args.workspace}[/dim]")
        
        return 0
        
    except QzAPIError as e:
        display.print_error(f"登录失败: {e}")
        return 1


def main():
    """主入口"""
    parser = argparse.ArgumentParser(
        prog="qzcli",
        description="启智平台任务管理 CLI 工具",
    )
    parser.add_argument(
        "--version", "-V",
        action="version",
        version=f"qzcli {__version__}"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="子命令")
    
    # init 命令
    init_parser = subparsers.add_parser("init", help="初始化配置")
    init_parser.add_argument("--username", "-u", help="用户名")
    init_parser.add_argument("--password", "-p", help="密码")
    
    # list 命令
    list_parser = subparsers.add_parser("list", aliases=["ls"], help="列出任务")
    list_parser.add_argument("--limit", "-n", type=int, default=20, help="显示数量限制")
    list_parser.add_argument("--status", "-s", help="按状态过滤")
    list_parser.add_argument("--running", "-r", action="store_true", help="只显示运行中/排队中的任务")
    list_parser.add_argument("--no-refresh", action="store_true", help="不更新状态")
    list_parser.add_argument("--verbose", "-v", action="store_true", help="显示详细信息")
    list_parser.add_argument("--url", "-u", action="store_true", default=True, help="显示任务链接（默认开启）")
    list_parser.add_argument("--wide", action="store_true", default=True, help="宽格式显示（默认开启）")
    list_parser.add_argument("--compact", action="store_true", help="紧凑表格格式（关闭宽格式）")
    # Cookie 模式参数
    list_parser.add_argument("--cookie", "-c", action="store_true", help="使用 cookie 从 API 获取任务（无需本地 store）")
    list_parser.add_argument("--workspace", "-w", help="工作空间（名称或 ID，cookie 模式）")
    list_parser.add_argument("--all-ws", action="store_true", help="查询所有已缓存的工作空间（cookie 模式）")
    
    # status 命令
    status_parser = subparsers.add_parser("status", aliases=["st"], help="查看任务状态")
    status_parser.add_argument("job_id", help="任务 ID")
    status_parser.add_argument("--json", "-j", action="store_true", help="输出 JSON")
    
    # stop 命令
    stop_parser = subparsers.add_parser("stop", help="停止任务")
    stop_parser.add_argument("job_id", help="任务 ID")
    stop_parser.add_argument("--yes", "-y", action="store_true", help="跳过确认")
    
    # watch 命令
    watch_parser = subparsers.add_parser("watch", aliases=["w"], help="实时监控")
    watch_parser.add_argument("--interval", "-i", type=int, default=10, help="刷新间隔（秒）")
    watch_parser.add_argument("--limit", "-n", type=int, default=30, help="显示数量限制")
    watch_parser.add_argument("--keep-alive", "-k", action="store_true", help="所有任务完成后继续监控")
    
    # track 命令（供脚本调用）
    track_parser = subparsers.add_parser("track", help="追踪任务")
    track_parser.add_argument("job_id", help="任务 ID")
    track_parser.add_argument("--name", help="任务名称")
    track_parser.add_argument("--source", help="来源脚本")
    track_parser.add_argument("--workspace", help="工作空间 ID")
    track_parser.add_argument("--quiet", "-q", action="store_true", help="静默模式")
    
    # import 命令
    import_parser = subparsers.add_parser("import", help="从文件导入任务")
    import_parser.add_argument("file", help="包含任务 ID 的文件")
    import_parser.add_argument("--source", help="来源标记")
    import_parser.add_argument("--refresh", "-r", action="store_true", help="导入后更新状态")
    
    # remove 命令
    remove_parser = subparsers.add_parser("remove", aliases=["rm"], help="删除任务记录")
    remove_parser.add_argument("job_id", help="任务 ID")
    remove_parser.add_argument("--yes", "-y", action="store_true", help="跳过确认")
    
    # clear 命令
    clear_parser = subparsers.add_parser("clear", help="清空所有任务记录")
    clear_parser.add_argument("--yes", "-y", action="store_true", help="跳过确认")
    
    # cookie 命令
    cookie_parser = subparsers.add_parser("cookie", help="设置浏览器 cookie（用于访问内部 API）")
    cookie_parser.add_argument("cookie", nargs="?", help="浏览器 cookie 字符串")
    cookie_parser.add_argument("--file", "-f", help="从文件读取 cookie")
    cookie_parser.add_argument("--workspace", "-w", help="默认工作空间 ID")
    cookie_parser.add_argument("--show", action="store_true", help="显示当前 cookie")
    cookie_parser.add_argument("--clear", action="store_true", help="清除 cookie")
    cookie_parser.add_argument("--no-test", action="store_true", help="不测试 cookie 有效性")
    
    # login 命令
    login_parser = subparsers.add_parser("login", help="通过 CAS 统一认证登录获取 cookie")
    login_parser.add_argument("--username", "-u", help="学工号")
    login_parser.add_argument("--password", "-p", help="密码")
    login_parser.add_argument("--workspace", "-w", help="默认工作空间 ID")
    
    # workspace 命令
    workspace_parser = subparsers.add_parser("workspace", aliases=["ws"], help="查看工作空间内所有运行任务")
    workspace_parser.add_argument("--workspace", "-w", help="工作空间 ID")
    workspace_parser.add_argument("--project", "-p", default="扩散", help="按项目名称过滤（默认: 扩散）")
    workspace_parser.add_argument("--all", "-a", action="store_true", help="显示所有项目（不过滤）")
    workspace_parser.add_argument("--page", type=int, default=1, help="页码")
    workspace_parser.add_argument("--size", type=int, default=100, help="每页数量（默认 100）")
    workspace_parser.add_argument("--sync", "-s", action="store_true", help="同步到本地任务列表")
    
    # workspaces 命令 - 从历史任务提取资源配置
    workspaces_parser = subparsers.add_parser("workspaces", aliases=["lsws", "res", "resources"], help="从历史任务提取资源配置（项目、计算组、规格）")
    workspaces_parser.add_argument("--workspace", "-w", help="工作空间 ID 或名称")
    workspaces_parser.add_argument("--export", "-e", action="store_true", help="输出可用于脚本的环境变量格式")
    workspaces_parser.add_argument("--update", "-u", action="store_true", help="强制从 API 更新缓存")
    workspaces_parser.add_argument("--list", "-l", action="store_true", help="列出所有已缓存的工作空间")
    workspaces_parser.add_argument("--name", help="设置工作空间名称（别名）")
    
    # avail 命令 - 查询空余节点
    avail_parser = subparsers.add_parser("avail", aliases=["av"], help="查询计算组空余节点，帮助决定任务应该提交到哪里")
    avail_parser.add_argument("--workspace", "-w", help="工作空间 ID 或名称")
    avail_parser.add_argument("--group", "-g", help="计算组 ID 或名称（可选，不指定则查询所有）")
    avail_parser.add_argument("--nodes", "-n", type=int, help="需要的节点数（推荐模式：找出满足条件的计算组）")
    avail_parser.add_argument("--export", "-e", action="store_true", help="输出可用于脚本的环境变量格式")
    avail_parser.add_argument("--verbose", "-v", action="store_true", help="显示空闲节点名称列表")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 0
    
    # 命令分发
    commands = {
        "init": cmd_init,
        "list": cmd_list,
        "ls": cmd_list,
        "status": cmd_status,
        "st": cmd_status,
        "stop": cmd_stop,
        "watch": cmd_watch,
        "w": cmd_watch,
        "track": cmd_track,
        "import": cmd_import,
        "remove": cmd_remove,
        "rm": cmd_remove,
        "clear": cmd_clear,
        "cookie": cmd_cookie,
        "login": cmd_login,
        "workspace": cmd_workspace,
        "ws": cmd_workspace,
        "workspaces": cmd_workspaces,
        "lsws": cmd_workspaces,
        "resources": cmd_workspaces,
        "res": cmd_workspaces,
        "avail": cmd_avail,
        "av": cmd_avail,
    }
    
    cmd_func = commands.get(args.command)
    if cmd_func:
        try:
            return cmd_func(args)
        except KeyboardInterrupt:
            print("\n操作已取消")
            return 130
        except Exception as e:
            display = get_display()
            display.print_error(str(e))
            return 1
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
