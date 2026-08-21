import logging
from models.models import get_session, ForwardRule, RuleSync, WebDAVAccount
from managers.state_manager import state_manager
from utils.common import get_ai_settings_text
from handlers import bot_handler
from utils.auto_delete import async_delete_user_message
from utils.common import get_bot_client
from utils.common import get_main_module
import traceback
from utils.auto_delete import send_message_and_delete
from models.models import PushConfig
from telethon import Button

logger = logging.getLogger(__name__)

async def handle_prompt_setting(event, client, sender_id, chat_id, current_state, message):
    """处理设置提示词的逻辑"""
    logger.info(f"开始处理提示词设置,用户ID:{sender_id},聊天ID:{chat_id},当前状态:{current_state}")
    
    if not current_state:
        logger.info("当前无状态,返回False")
        return False

    rule_id = None
    field_name = None 
    prompt_type = None
    template_type = None

    if current_state.startswith("set_summary_prompt:"):
        rule_id = current_state.split(":")[1]
        field_name = "summary_prompt"
        prompt_type = "AI总结"
        template_type = "ai"
        logger.info(f"检测到设置总结提示词,规则ID:{rule_id}")
    elif current_state.startswith("set_ai_prompt:"):
        rule_id = current_state.split(":")[1]
        field_name = "ai_prompt"
        prompt_type = "AI"
        template_type = "ai"
        logger.info(f"检测到设置AI提示词,规则ID:{rule_id}")
    elif current_state.startswith("webdav_set_api:"):
        account_id = int(current_state.split(":")[1])
        logger.info(f"检测到设置 WebDAV API 代理域名,账号ID:{account_id}")
        session = get_session()
        try:
            account = session.query(WebDAVAccount).get(account_id)
            if not account:
                await event.answer('账号不存在')
                return True

            api_url = event.message.text.strip()
            if api_url.lower() in ('/cancel', '取消'):
                account.api_base_url = None
                session.commit()
                await message.edit(
                    f"已清除 API 代理域名",
                    buttons=[[Button.inline('👈 返回', f'webdav_settings:{account.id}')]]
                )
            else:
                # 清理 URL 格式
                if api_url and not api_url.startswith('http'):
                    api_url = f'https://{api_url}'
                api_url = api_url.rstrip('/')
                account.api_base_url = api_url
                session.commit()
                await message.edit(
                    f"✅ API 代理域名已设置为: {api_url}",
                    buttons=[[Button.inline('👈 返回', f'webdav_settings:{account.id}')]]
                )

            state_manager.clear_state(sender_id, chat_id)
            await async_delete_user_message(client, event.message.chat_id, event.message.id, 0)
        except Exception as e:
            session.rollback()
            logger.error(f"设置 WebDAV API 代理域名失败: {e}")
        finally:
            session.close()
        return True
    elif current_state.startswith("set_userinfo_template:"):
        rule_id = current_state.split(":")[1]
        field_name = "userinfo_template"
        prompt_type = "用户信息"
        template_type = "userinfo"
        logger.info(f"检测到设置用户信息模板,规则ID:{rule_id}")
    elif current_state.startswith("set_time_template:"):
        rule_id = current_state.split(":")[1]
        field_name = "time_template"
        prompt_type = "时间"
        template_type = "time"
        logger.info(f"检测到设置时间模板,规则ID:{rule_id}")
    elif current_state.startswith("set_original_link_template:"):
        rule_id = current_state.split(":")[1]
        field_name = "original_link_template"
        prompt_type = "原始链接"
        template_type = "link"
        logger.info(f"检测到设置原始链接模板,规则ID:{rule_id}")
    elif current_state.startswith("add_push_channel:"):
        # 处理添加推送频道
        rule_id = current_state.split(":")[1]
        logger.info(f"检测到添加推送频道,规则ID:{rule_id}")
        return await handle_add_push_channel(event, client, sender_id, chat_id, rule_id, message)
    else:
        logger.info(f"未知的状态类型:{current_state}")
        return False

    logger.info(f"处理设置{prompt_type}提示词/模板,规则ID:{rule_id},字段名:{field_name}")
    session = get_session()
    try:
        logger.info(f"查询规则ID:{rule_id}")
        rule = session.query(ForwardRule).get(int(rule_id))
        if rule:
            old_prompt = getattr(rule, field_name) if hasattr(rule, field_name) else None
            new_prompt = event.message.text
            logger.info(f"找到规则,原提示词/模板:{old_prompt}")
            logger.info(f"准备更新为新提示词/模板:{new_prompt}")
            
            setattr(rule, field_name, new_prompt)
            session.commit()
            logger.info(f"已更新规则{rule_id}的{prompt_type}提示词/模板")

            # 检查是否启用了同步功能
            if rule.enable_sync:
                logger.info(f"规则 {rule.id} 启用了同步功能，正在同步提示词/模板设置到关联规则")
                # 获取需要同步的规则列表
                sync_rules = session.query(RuleSync).filter(RuleSync.rule_id == rule.id).all()
                
                # 为每个同步规则应用相同的提示词设置
                for sync_rule in sync_rules:
                    sync_rule_id = sync_rule.sync_rule_id
                    logger.info(f"正在同步{prompt_type}提示词/模板到规则 {sync_rule_id}")
                    
                    # 获取同步目标规则
                    target_rule = session.query(ForwardRule).get(sync_rule_id)
                    if not target_rule:
                        logger.warning(f"同步目标规则 {sync_rule_id} 不存在，跳过")
                        continue
                    
                    # 更新同步目标规则的提示词设置
                    try:
                        # 记录旧提示词
                        old_target_prompt = getattr(target_rule, field_name) if hasattr(target_rule, field_name) else None
                        
                        # 设置新提示词
                        setattr(target_rule, field_name, new_prompt)
                        
                        logger.info(f"同步规则 {sync_rule_id} 的{prompt_type}提示词/模板从 '{old_target_prompt}' 到 '{new_prompt}'")
                    except Exception as e:
                        logger.error(f"同步{prompt_type}提示词/模板到规则 {sync_rule_id} 时出错: {str(e)}")
                        continue
                
                session.commit()
                logger.info("所有同步提示词/模板更改已提交")
            
            logger.info(f"清除用户状态,用户ID:{sender_id},聊天ID:{chat_id}")
            state_manager.clear_state(sender_id, chat_id)
            
            
            message_chat_id = event.message.chat_id
            bot_client = await get_bot_client()
            
            
            try:
                await async_delete_user_message(bot_client, message_chat_id, event.message.id, 0)
            except Exception as e:
                logger.error(f"删除用户消息失败: {str(e)}")

            await message.delete()
            logger.info("准备发送更新后的设置消息")
            
            # 根据模板类型选择不同的显示页面
            if template_type == "ai":
                # AI设置页面
                await client.send_message(
                    chat_id,
                    await get_ai_settings_text(rule),
                    buttons=await bot_handler.create_ai_settings_buttons(rule)
                )
            elif template_type in ["userinfo", "time", "link"]:
                # 其他设置页面
                await client.send_message(
                    chat_id,
                    f"已更新规则 {rule_id} 的{prompt_type}模板",
                    buttons=await bot_handler.create_other_settings_buttons(rule_id=rule_id)
                )
            
            # 删除用户消息
            logger.info("设置消息发送成功")
            return True
        else:
            logger.warning(f"未找到规则ID:{rule_id}")
    except Exception as e:
        logger.error(f"处理提示词/模板设置时发生错误:{str(e)}")
        raise
    finally:
        session.close()
        logger.info("数据库会话已关闭")
    return True

async def handle_add_push_channel(event, client, sender_id, chat_id, rule_id, message):
    """处理添加推送频道的逻辑"""
    logger.info(f"开始处理添加推送频道,规则ID:{rule_id}")
    
    session = get_session()
    try:
        # 获取规则
        rule = session.query(ForwardRule).get(int(rule_id))
        if not rule:
            logger.warning(f"未找到规则ID:{rule_id}")
            return False
        
        # 获取用户输入的推送频道信息
        push_channel = event.message.text.strip()
        logger.info(f"用户输入的推送频道: {push_channel}")
        
        try:
            # 创建新的推送配置
            is_email = push_channel.startswith(('mailto://', 'mailtos://', 'email://'))
            push_config = PushConfig(
                rule_id=int(rule_id),
                push_channel=push_channel,
                enable_push_channel=True,
                media_send_mode="Multiple" if is_email else "Single"
            )
            session.add(push_config)
            
            # 启用规则的推送功能
            rule.enable_push = True
            
            # 检查是否启用了同步功能
            if rule.enable_sync:
                logger.info(f"规则 {rule.id} 启用了同步功能，正在同步推送配置到关联规则")
                
                # 获取需要同步的规则列表
                sync_rules = session.query(RuleSync).filter(RuleSync.rule_id == rule.id).all()
                
                # 为每个同步规则创建相同的推送配置
                for sync_rule in sync_rules:
                    sync_rule_id = sync_rule.sync_rule_id
                    logger.info(f"正在同步推送配置到规则 {sync_rule_id}")
                    
                    # 获取同步目标规则
                    target_rule = session.query(ForwardRule).get(sync_rule_id)
                    if not target_rule:
                        logger.warning(f"同步目标规则 {sync_rule_id} 不存在，跳过")
                        continue
                    
                    # 检查目标规则是否已存在相同推送频道
                    existing_config = session.query(PushConfig).filter_by(
                        rule_id=sync_rule_id, 
                        push_channel=push_channel
                    ).first()
                    
                    if existing_config:
                        logger.info(f"目标规则 {sync_rule_id} 已存在推送频道 {push_channel}，跳过")
                        continue
                    
                    # 创建新的推送配置
                    try:
                        sync_push_config = PushConfig(
                            rule_id=sync_rule_id,
                            push_channel=push_channel,
                            enable_push_channel=True,
                            media_send_mode=push_config.media_send_mode
                        )
                        session.add(sync_push_config)
                        
                        # 启用目标规则的推送功能
                        target_rule.enable_push = True
                        
                        logger.info(f"已为规则 {sync_rule_id} 添加推送频道 {push_channel}")
                    except Exception as e:
                        logger.error(f"为规则 {sync_rule_id} 添加推送配置时出错: {str(e)}")
                        continue
            
            # 提交更改
            session.commit()
            success = True
            message_text = "成功添加推送配置"
        except Exception as db_error:
            session.rollback()
            success = False
            message_text = f"添加推送配置失败: {str(db_error)}"
            logger.error(f"添加推送配置到数据库时出错: {str(db_error)}")
        
        # 清除状态
        state_manager.clear_state(sender_id, chat_id)
        
        # 删除用户消息
        message_chat_id = event.message.chat_id
        bot_client = await get_bot_client()
        try:
            await async_delete_user_message(bot_client, message_chat_id, event.message.id, 0)
        except Exception as e:
            logger.error(f"删除用户消息失败: {str(e)}")
        
        # 删除原始消息并显示结果
        await message.delete()
        
        # 获取主界面
        main_module = await get_main_module()
        bot_client = main_module.bot_client
        
        # 发送结果通知
        if success:
            await send_message_and_delete(
                bot_client,
                chat_id,
                f"已成功添加推送频道: {push_channel}",
                buttons=await bot_handler.create_push_settings_buttons(rule_id)
            )
        else:
            await send_message_and_delete(
                bot_client,
                chat_id,
                f"添加推送频道失败: {message_text}",
                buttons=await bot_handler.create_push_settings_buttons(rule_id)
            )
        
        return True
    except Exception as e:
        logger.error(f"处理添加推送频道时出错: {str(e)}")
        logger.error(traceback.format_exc())
        return False
    finally:
        session.close()