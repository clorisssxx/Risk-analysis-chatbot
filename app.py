from flask import Flask, render_template, request, jsonify
import pandas as pd
import jieba
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import re
from datetime import datetime
import cx_Oracle
import os

app = Flask(__name__)

DB_CONFIG = {
    'user': 'XXX',
    'password': 'oracle',
    'dsn': 'XXX'
}

def get_db_connection():
    """获取数据库连接"""
    return cx_Oracle.connect(
        user=DB_CONFIG['user'],        
        password=DB_CONFIG['password'],
        dsn=DB_CONFIG['dsn']           
    )
   
def chinese_tokenizer(text):
    return list(jieba.cut(text))
  
   
##################Account Profits Analysis####################
class DailyProfitLossQueryProcessor:
    """Daily, Monthly, Yearly Loss"""
    
    def __init__(self, conn):
        self.conn = conn
    
    def _extract_profit_loss_info_from_query(self, user_query):
        """Extract unit, group, date, and query type from query"""
        import re
        
        if any(keyword in user_query for keyword in ['月盈亏', '月度盈亏', '当月盈亏']):
            query_type = "monthly"
        elif any(keyword in user_query for keyword in ['年盈亏', '年度盈亏', '当年盈亏']):
            query_type = "yearly"
        else:
            query_type = "daily"
            
        has_latest_keyword = any(keyword in user_query for keyword in ['最新', '当日', '今天', '今日', '最近'])
        
        patterns = [
            
            r'([\u4e00-\u9fa5a-zA-Z0-9]{2,20})在(\d{4}-\d{1,2}-\d{1,2})的(?:日盈亏|总盈亏|当日盈亏|每日盈亏)',
            r'([\u4e00-\u9fa5a-zA-Z0-9]{2,20})在(\d{4}-\d{1,2})的(?:月盈亏|月度盈亏|当月盈亏)',
            r'([\u4e00-\u9fa5a-zA-Z0-9]{2,20})在(\d{4})年的(?:年盈亏|年度盈亏|当年盈亏)',
                     
            r'在(\d{4}-\d{1,2}-\d{1,2})[\s的]*([\u4e00-\u9fa5a-zA-Z0-9]{2,20})[\s的]*(?:日盈亏|总盈亏)',
            r'在(\d{4}-\d{1,2})[\s的]*([\u4e00-\u9fa5a-zA-Z0-9]{2,20})[\s的]*(?:月盈亏|月度盈亏)',
            r'在(\d{4})年[\s的]*([\u4e00-\u9fa5a-zA-Z0-9]{2,20})[\s的]*(?:年盈亏|年度盈亏)',
                   
            r'([\u4e00-\u9fa5a-zA-Z0-9]{2,20})在(\d{4})年(\d{1,2})月(\d{1,2})日[\s的]*(?:日盈亏|总盈亏)',
            r'([\u4e00-\u9fa5a-zA-Z0-9]{2,20})在(\d{4})年(\d{1,2})月[\s的]*(?:月盈亏|月度盈亏)',
            r'([\u4e00-\u9fa5a-zA-Z0-9]{2,20})在(\d{4})年[\s的]*(?:年盈亏|年度盈亏)',            
          
            r'([\u4e00-\u9fa5a-zA-Z0-9]{2,20})[\s的]*.*?(\d{4}-\d{1,2}-\d{1,2})[\s的]*.*?(?:日盈亏|总盈亏)',
            r'([\u4e00-\u9fa5a-zA-Z0-9]{2,20})[\s的]*.*?(\d{4}-\d{1,2})[\s的]*.*?(?:月盈亏|月度盈亏)',
            r'([\u4e00-\u9fa5a-zA-Z0-9]{2,20})[\s的]*.*?(\d{4})[\s的]*.*?(?:年盈亏|年度盈亏)',            
           
            r'([\u4e00-\u9fa5]{2,10})[的]*(?:日盈亏|月盈亏|年盈亏).*?(\d{4}(?:-\d{1,2}(?:-\d{1,2})?)?)'
        ]
            
        for pattern in patterns:
            match = re.search(pattern, user_query)
            if match:
                groups = match.groups()
                print(f"Match: {pattern}, Group: {groups}")       
                if len(groups) == 2:
                    
                    if (re.match(r'^[\u4e00-\u9fa5a-zA-Z0-9]{2,20}$', groups[0].strip()) and 
                        re.match(r'^\d{4}(?:-\d{1,2}(?:-\d{1,2})?)?$', groups[1].strip())):
                        unit_name = groups[0].strip()
                        date_str = groups[1].strip()           
                    
                    elif (re.match(r'^\d{4}(?:-\d{1,2}(?:-\d{1,2})?)?$', groups[0].strip()) and
                          re.match(r'^[\u4e00-\u9fa5a-zA-Z0-9]{2,20}$', groups[1].strip())):
                        date_str = groups[0].strip()
                        unit_name = groups[1].strip()
                    else:
                        continue
                  
                    if re.match(r'^\d{4}-\d{1,2}-\d{1,2}$', date_str):
                        query_type = "daily"
                    elif re.match(r'^\d{4}-\d{1,2}$', date_str):
                        query_type = "monthly"
                    elif re.match(r'^\d{4}$', date_str):
                        query_type = "yearly"
                
                #fFormats like "2025年10月9日"
                elif len(groups) == 4:
                    if re.match(r'^[\u4e00-\u9fa5a-zA-Z0-9]{2,20}$', groups[0].strip()):
                        unit_name = groups[0].strip()
                        year, month, day = groups[1], groups[2], groups[3]
                        date_str = f"{year}-{month}-{day}"
                        query_type = "daily"
                    else:
                        year, month, day, unit_name = groups[0], groups[1], groups[2], groups[3]
                        date_str = f"{year}-{month}-{day}"
                        query_type = "daily"
                else:
                    continue
            
                date_str = self._normalize_extracted_date(date_str, query_type)
                if date_str:
                    return {
                        'unit_name': unit_name,
                        'date': date_str,
                        'query_type': query_type
                    }
        
        if has_latest_keyword:
            print("检测到最新查询关键词")            
            unit_patterns = [
                r'([\u4e00-\u9fa5]{2,10})[\s的]*(?:最新|当日|今天|今日|最近)',
                r'(?:最新|当日|今天|今日|最近)[\s的]*([\u4e00-\u9fa5]{2,10})',
                r'([\u4e00-\u9fa5a-zA-Z0-9]{2,20})'
            ]            
            unit_name = None
            for pattern in unit_patterns:
                match = re.search(pattern, user_query)
                if match:
                    unit_name = match.group(1)
                    break            
            if unit_name:
                print(f"找到单元名称: {unit_name}")               
                if query_type == "daily":
                    placeholder_date = datetime.now().strftime('%Y-%m-%d')
                elif query_type == "monthly":
                    placeholder_date = datetime.now().strftime('%Y-%m')
                elif query_type == "yearly":
                    placeholder_date = datetime.now().strftime('%Y')                
                return {
                    'unit_name': unit_name,
                    'date': placeholder_date,
                    'query_type': query_type,
                    'is_latest': True
                }        
        return self._fallback_extract_profit_loss_info(user_query)

    
    def _fallback_extract_profit_loss_info(self, user_query):
        """备选提取方法：基于关键词分词"""
        import re

        if any(keyword in user_query for keyword in ['月盈亏', '月度盈亏', '当月盈亏']):
            query_type = "monthly"
        elif any(keyword in user_query for keyword in ['年盈亏', '年度盈亏', '当年盈亏']):
            query_type = "yearly"
        else:
            query_type = "daily"
        unit_pattern = r'([\u4e00-\u9fa5a-zA-Z0-9]{2,20})'
        unit_match = re.search(unit_pattern, user_query) 
        if unit_match:
            unit_name = unit_match.group(1)
        else:
            return None        
            
        date_patterns = [
            r'\d{4}-\d{1,2}-\d{1,2}',  # YYYY-MM-DD
            r'\d{4}-\d{1,2}',  # YYYY-MM
            r'\d{4}',  # YYYY
            r'\d{4}年\d{1,2}月\d{1,2}日',  # YYYY年MM月DD日
            r'\d{4}年\d{1,2}月',  # YYYY年MM月
            r'\d{4}年',  # YYYY年
        ]
        found_date = None
        for pattern in date_patterns:
            match = re.search(pattern, user_query)
            if match:
                found_date = match.group()
                break       
        if not found_date:
            return None
            
        # 标准化日期
        found_date = self._normalize_extracted_date(found_date, query_type)
        if found_date:
            return {
                'unit_name': unit_name,
                'date': found_date,
                'query_type': query_type
            }
        return None
    
    def _normalize_extracted_date(self, date_str, query_type):
        import re
        from datetime import datetime
        try:
            # YYYY-MM-DD
            if query_type == "daily":
                if re.match(r'\d{4}-\d{1,2}-\d{1,2}', date_str):
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                    return date_obj.strftime('%Y-%m-%d')
                elif re.match(r'\d{4}年\d{1,2}月\d{1,2}日', date_str):
                    date_str = date_str.replace('年', '-').replace('月', '-').replace('日', '')
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                    return date_obj.strftime('%Y-%m-%d')
            # YYYY-MM
            elif query_type == "monthly":
                if re.match(r'\d{4}-\d{1,2}', date_str):
                    # 确保月份格式正确
                    year, month = date_str.split('-')
                    return f"{year}-{month:0>2}"
                elif re.match(r'\d{4}年\d{1,2}月', date_str):
                    date_str = date_str.replace('年', '-').replace('月', '')
                    year, month = date_str.split('-')
                    return f"{year}-{month:0>2}"
            # YYYY
            elif query_type == "yearly":
                if re.match(r'\d{4}', date_str):
                    return date_str
                elif re.match(r'\d{4}年', date_str):
                    return date_str.replace('年', '')       
        except ValueError:
            return None
        return None
    
    def _get_daily_profit_loss(self, unit_name, date, is_latest=False):
        """根据单元名称和日期查询日盈亏"""
        try:
            if is_latest:
                query = """
                    SELECT * FROM (
                    SELECT 
                        单元名称, 组合名称, 结算单日期, SUM(总盈亏) as 总盈亏 
                    FROM 
                        SPECIES_CLASSIFY_PROFIT 
                    WHERE 
                         (单元名称 = :unit_name OR 组合名称 = :unit_name) 
                    GROUP BY 单元名称, 组合名称,结算单日期
                    ORDER BY 结算单日期 DESC
                ) WHERE ROWNUM <= 1
                """
                result = pd.read_sql(query, self.conn, params={
                    'unit_name': unit_name
                })    
            else:
                query = """
                SELECT 单元名称, 组合名称, 结算单日期, SUM(总盈亏) as 总盈亏 
                FROM SPECIES_CLASSIFY_PROFIT 
                WHERE  (单元名称 = :unit_name OR 组合名称 = :unit_name) AND 结算单日期 = TO_DATE(:date_str, 'YYYY-MM-DD')
                GROUP BY 单元名称, 组合名称, 结算单日期
                """
                result = pd.read_sql(query, self.conn, params={
                    'unit_name': unit_name, 
                    'date_str': date
                })
                if result.empty:
                    print(f"未找到 {unit_name} 在 {date} 的精确日盈亏信息，进行模糊匹配")
                    return None         
            if not result.empty:
                actual_date = result.iloc[0]['结算单日期']
                if is_latest:
                    print(f"找到 {unit_name} 最新日期 {actual_date} 的日盈亏信息")
                else:
                    print(f"找到 {unit_name} 在 {date} 的日盈亏信息，共 {len(result)} 条记录")
                return result
            else:
                print(f"未找到 {unit_name} 的日盈亏信息")
                return None   
        except Exception as e:
            print(f"查询日盈亏时出错: {e}")
            return None
    
    def _get_monthly_profit_loss(self, unit_name, month, is_latest=False):
        """根据单元名称和年月查询月盈亏"""
        try:
            if is_latest:
                query = """
                SELECT * FROM (
                    SELECT 
                        单元名称, 组合名称,
                        TO_CHAR(结算单日期, 'YYYY-MM') AS 当月,
                        SUM(总盈亏) AS 月盈亏                       
                    FROM 
                        SPECIES_CLASSIFY_PROFIT 
                    WHERE 
                         (单元名称 = :unit_name OR 组合名称 = :unit_name) 
                    GROUP BY 
                        单元名称, 组合名称, TO_CHAR(结算单日期, 'YYYY-MM')
                    ORDER BY TO_CHAR(结算单日期, 'YYYY-MM') DESC
                ) WHERE ROWNUM <= 1
                """
                params = {'unit_name': unit_name}
            else:    
                query = """
                SELECT 
                    单元名称, 组合名称,
                    TO_CHAR(结算单日期, 'YYYY-MM') AS 当月,
                    SUM(总盈亏) AS 月盈亏                  
                FROM 
                    SPECIES_CLASSIFY_PROFIT 
                WHERE 
                     (单元名称 = :unit_name OR 组合名称 = :unit_name) AND TO_CHAR(结算单日期, 'YYYY-MM') = :month
                GROUP BY 
                    单元名称, 组合名称, TO_CHAR(结算单日期, 'YYYY-MM')
                """
                params = {'unit_name': unit_name, 'month': month}       
            result = pd.read_sql(query, self.conn, params=params)  
            if not result.empty:
                actual_month = result.iloc[0]['当月']
                if is_latest:
                    print(f"找到 {unit_name} 最新月份 {actual_month} 的月盈亏信息")
                else:
                    print(f"找到 {unit_name} 在 {month} 的月盈亏信息")
                return result
            else:
                print(f"未找到 {unit_name} 的月盈亏信息")
                return None
        except Exception as e:
            print(f"查询月盈亏时出错: {e}")
            return None
    
    def _get_yearly_profit_loss(self, unit_name, year, is_latest=False):
        """根据单元名称和年份查询年盈亏"""
        try:
            if is_latest:
                query = """
                SELECT * FROM (
                    SELECT 
                        单元名称,
                        组合名称,
                        TO_CHAR(结算单日期, 'YYYY') AS 今年,
                        SUM(总盈亏) AS 年盈亏
                    FROM 
                        SPECIES_CLASSIFY_PROFIT 
                    WHERE 
                         (单元名称 = :unit_name OR 组合名称 = :unit_name) 
                    GROUP BY 
                        单元名称, 组合名称, TO_CHAR(结算单日期, 'YYYY')
                    ORDER BY TO_CHAR(结算单日期, 'YYYY') DESC, 组合名称
                ) WHERE ROWNUM <= 1
                """
                params = {'unit_name': unit_name}
            else:
                query = """
                SELECT 
                    单元名称, 组合名称,
                    TO_CHAR(结算单日期, 'YYYY') AS 今年,
                    SUM(总盈亏) AS 年盈亏
                FROM 
                    SPECIES_CLASSIFY_PROFIT 
                WHERE 
                     (单元名称 = :unit_name OR 组合名称 = :unit_name) AND TO_CHAR(结算单日期, 'YYYY') = :year
                GROUP BY 
                    单元名称, 组合名称, TO_CHAR(结算单日期, 'YYYY')
                """
                params = {'unit_name': unit_name, 'year': year}
            result = pd.read_sql(query, self.conn, params=params)
            if not result.empty:
                actual_year = result.iloc[0]['今年']
                if is_latest:
                    print(f"找到 {unit_name} 最新年份 {actual_year} 的年盈亏信息")
                else:
                    print(f"找到 {unit_name} 在 {year} 年的年盈亏信息")
                return result
            else:
                print(f"未找到 {unit_name} 的年盈亏信息")
                return None              
        except Exception as e:
            print(f"查询年盈亏时出错: {e}")
            return None
    
    def _format_profit_loss_dataframe(self, df, query_type):
        """格式化盈亏DataFrame，使其更整齐易读"""
        if df is None or df.empty:
            return df
        formatted_df = df.reset_index(drop=True)
        formatted_df.columns = [str(col) for col in formatted_df.columns]
        formatted_df = formatted_df.fillna('')
        return formatted_df
    
    def process_profit_loss_query(self, user_query):
        """处理盈亏查询的主方法"""
        print(f"【盈亏查询模块】开始处理用户问题: {user_query}")

        profit_loss_info = self._extract_profit_loss_info_from_query(user_query)
        if not profit_loss_info:
            return {
                'status': 'error',
                'message': "请提供具体的单元名称和日期，例如：\n- 期现产业2025-10-09的日盈亏\n- 期现产业2025-10的月盈亏\n- 期现产业2025年的年盈亏\n- 期现单元最新日盈亏"
            }
            
        unit_name = profit_loss_info['unit_name']
        date = profit_loss_info['date']
        query_type = profit_loss_info['query_type']
        is_latest = profit_loss_info.get('is_latest', False)
        
        print(f"从用户提问提取到信息：单元: {unit_name}, 日期: {date}, 查询类型: {query_type}, 是否最新: {is_latest}")
        
        if query_type == "daily":
            profit_loss_data = self._get_daily_profit_loss(unit_name, date, is_latest)
            value_column = "总盈亏"
            display_name = "日盈亏"
        elif query_type == "monthly":
            profit_loss_data = self._get_monthly_profit_loss(unit_name, date, is_latest)
            value_column = "月盈亏"
            display_name = "月盈亏"
        elif query_type == "yearly":
            profit_loss_data = self._get_yearly_profit_loss(unit_name, date, is_latest)
            value_column = "年盈亏"
            display_name = "年盈亏"
            
        if profit_loss_data is not None and not profit_loss_data.empty:
            formatted_data = self._format_profit_loss_dataframe(profit_loss_data, query_type)
            total_profit_loss = profit_loss_data[value_column].sum()
            if is_latest:
                if query_type == "daily":
                    actual_date = profit_loss_data.iloc[0]['结算单日期']
                    date_display = f"最新日期({actual_date.strftime('%Y-%m-%d')})"
                elif query_type == "monthly":
                    actual_date = profit_loss_data.iloc[0]['当月']
                    date_display = f"最新月份({actual_date})"
                elif query_type == "yearly":
                    actual_date = profit_loss_data.iloc[0]['今年']
                    date_display = f"最新年份({actual_date})"
            else:
                date_display = date
                
            if total_profit_loss > 0:
                profit_status = "盈利"
            elif total_profit_loss < 0:
                profit_status = "亏损"
            else:
                profit_status = "持平"
                
            if query_type == "daily":
                message = f"{unit_name}在{date_display}的{display_name}为: {total_profit_loss:,.2f}元 ({profit_status})"
            elif query_type == "monthly":
                message = f"{unit_name}在{date_display}的{display_name}为: {total_profit_loss:,.2f}元 ({profit_status})"
            elif query_type == "yearly":
                message = f"{unit_name}在{date_display}的{display_name}为: {total_profit_loss:,.2f}元 ({profit_status})"
            return {
                'status': 'success',
                'unit_name': unit_name,
                'date': date_display,
                'query_type': query_type,
                'is_latest': is_latest,
                'total_profit_loss': total_profit_loss,
                'profit_status': profit_status,
                'data': formatted_data,
                'message': message
            }
        else:
            return {
                'status': 'not_found',
                'unit_name': unit_name,
                'date': date,
                'query_type': query_type,
                'is_latest': is_latest,
                'message': f"未找到{unit_name}的{display_name}信息"
            }
            
################Account Fund Analysis###################
class AccountFundsQueryProcessor:
    """独立处理账户资金查询的模块"""
    
    def __init__(self, conn):
        self.conn = conn
    
    def _extract_fund_info_from_query(self, user_query):
        """从用户查询中提取组合名称和日期信息"""
        import re
        
        patterns = [
            # 组合在前的模式
            r'([\u4e00-\u9fa5a-zA-Z0-9]+)组合在(\d{4}-\d{1,2}-\d{1,2})的(?:账户资金|可用资金)',
            r'([\u4e00-\u9fa5a-zA-Z0-9]+)组合(\d{4}-\d{1,2}-\d{1,2})的(?:账户资金|可用资金)',
            r'查询([\u4e00-\u9fa5a-zA-Z0-9]+)组合在(\d{4}-\d{1,2}-\d{1,2})的(?:账户资金|可用资金)',
            r'([\u4e00-\u9fa5a-zA-Z0-9]+)在(\d{4}-\d{1,2}-\d{1,2})的(?:账户资金|可用资金)',
            
            # 日期在前的模式
            r'(\d{4}-\d{1,2}-\d{1,2})([\u4e00-\u9fa5a-zA-Z0-9]+)组合的(?:账户资金|可用资金)',
            r'(\d{4}-\d{1,2}-\d{1,2})的([\u4e00-\u9fa5a-zA-Z0-9]+)组合(?:账户资金|可用资金)',
            
            # 年月日格式
            r'([\u4e00-\u9fa5a-zA-Z0-9]+)组合在(\d{4})年(\d{1,2})月(\d{1,2})日的(?:账户资金|可用资金)',
            r'([\u4e00-\u9fa5a-zA-Z0-9]+)组合(\d{4})年(\d{1,2})月(\d{1,2})日的(?:账户资金|可用资金)',
            r'(\d{4})年(\d{1,2})月(\d{1,2})日([\u4e00-\u9fa5a-zA-Z0-9]+)组合的(?:账户资金|可用资金)',
            
            # 灵活匹配模式
            r'([\u4e00-\u9fa5a-zA-Z0-9]{2,10})组合.*?(?:账户资金|可用资金).*?(\d{4}-\d{1,2}-\d{1,2})',
            r'(?:账户资金|可用资金).*?([\u4e00-\u9fa5a-zA-Z0-9]{2,10})组合.*?(\d{4}-\d{1,2}-\d{1,2})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, user_query)
            if match:
                groups = match.groups()
                if len(groups) == 2:
                    group1, group2 = groups
                    if (re.match(r'^[\u4e00-\u9fa5a-zA-Z0-9]{2,20}$', group1.strip()) and 
                        re.match(r'^\d{4}-\d{1,2}-\d{1,2}$', group2.strip())):
                        portfolio = group1.strip()
                        date_str = group2.strip()
                    elif (re.match(r'^\d{4}-\d{1,2}-\d{1,2}$', group1.strip()) and 
                          re.match(r'^[\u4e00-\u9fa5a-zA-Z0-9]{2,20}$', group2.strip())):
                        portfolio = group2.strip()
                        date_str = group1.strip()
                    else:
                        continue
            
                elif len(groups) == 4:
                    if re.match(r'^[\u4e00-\u9fa5a-zA-Z0-9]{2,20}$', groups[0].strip()):
                        portfolio = groups[0].strip()
                        year, month, day = groups[1], groups[2], groups[3]
                        date_str = f"{year}-{month}-{day}"
                    else:
                        year, month, day, portfolio = groups[0], groups[1], groups[2], groups[3]
                        date_str = f"{year}-{month}-{day}"
                else:
                    continue

                date_str = self._normalize_extracted_date(date_str)
                if date_str:
                    return {
                        'portfolio': portfolio,
                        'date': date_str
                    }
        
        return self._fallback_extract_fund_info(user_query)
    
    def _fallback_extract_fund_info(self, user_query):
        """备选提取方法：基于关键词分词"""
        import re
        portfolio_pattern = r'[\u4e00-\u9fa5a-zA-Z0-9]{2,20}(?=组合)'
        portfolio_match = re.search(portfolio_pattern, user_query)
        if not portfolio_match:
            return None       
        portfolio = portfolio_match.group()

        date_patterns = [
            r'\d{4}-\d{1,2}-\d{1,2}',  # YYYY-MM-DD
            r'\d{4}年\d{1,2}月\d{1,2}日',  # YYYY年MM月DD日
        ]
        
        found_date = None
        for pattern in date_patterns:
            match = re.search(pattern, user_query)
            if match:
                found_date = match.group()
                break      
        if not found_date:
            return None
        
        found_date = self._normalize_extracted_date(found_date)
        if found_date:
            return {
                'portfolio': portfolio,
                'date': found_date
            }
        
        return None
    
    def _normalize_extracted_date(self, date_str):
        """标准化提取的日期格式为 YYYY-MM-DD"""
        import re
        from datetime import datetime
        
        try:
            if re.match(r'\d{4}-\d{1,2}-\d{1,2}', date_str):
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                return date_obj.strftime('%Y-%m-%d')
            elif re.match(r'\d{4}年\d{1,2}月\d{1,2}日', date_str):
                date_str = date_str.replace('年', '-').replace('月', '-').replace('日', '')
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            return None       
        return None
    
    def _get_account_funds(self, portfolio, date):
        """根据组合名称和日期查询账户资金"""
        try:
            query = """
            SELECT 组合名称, 日期, 可用资金 
            FROM STOCK_OPTION_EQUITY
            WHERE 组合名称 LIKE :portfolio AND 日期 = TO_DATE(:date_str, 'YYYY-MM-DD')
            """
            result = pd.read_sql(query, self.conn, params={'portfolio': f'%{portfolio}%', 'date_str': date})
            
            if not result.empty:
                print(f"找到 {portfolio} 在 {date} 的资金信息，共 {len(result)} 条记录")
                return result
            else:
                print(f"未找到 {portfolio} 在 {date} 的精确资金信息，进行模糊匹配")
                return self._get_latest_account_funds(portfolio, date)
                
        except Exception as e:
            print(f"查询账户资金时出错: {e}")
            return None
    
    
    def _get_latest_account_funds(self, portfolio, date):
        """获取指定日期最新的账户资金，如果当天没有数据则找最近的数据"""
        try:
            
            query = """
            SELECT * FROM (
                SELECT 组合名称, 日期, 可用资金 
                FROM STOCK_OPTION_EQUITY
                WHERE 组合名称 LIKE :portfolio AND 日期 <= TO_DATE(:date_str, 'YYYY-MM-DD')
                ORDER BY 日期 DESC 
            ) WHERE ROWNUM <= 1
            """
            result = pd.read_sql(query, self.conn, params={'portfolio': f'%{portfolio}%', 'date_str': date})
            
            if not result.empty:
                actual_date = result.iloc[0]['日期']
                print(f"返回 {portfolio} 最近日期 {actual_date} 的资金数据")
                return result
            else:
                print(f"未找到 {portfolio} 在任何日期的资金信息")
                return None
                
        except Exception as e:
            print(f"查询最新资金信息时出错: {e}")
            return None
    
    
    def _format_funds_dataframe(self, df):
        """格式化资金DataFrame，使其更整齐易读"""
        if df is None or df.empty:
            return df
        formatted_df = df.reset_index(drop=True)
        formatted_df.columns = [str(col) for col in formatted_df.columns]
        formatted_df = formatted_df.fillna('')
        return formatted_df
    
    
    def process_funds_query(self, user_query):
        """处理资金查询的主方法"""
        print(f"【资金模块】开始处理用户问题: {user_query}")
        fund_info = self._extract_fund_info_from_query(user_query)
        if not fund_info:
            return {
                'status': 'error',
                'message': "请提供具体的组合名称和日期，例如：XX组合2025-10-09的账户资金是多少。如寻找某交易员管理账户资金，请在交易员名字后添加组合二字"
            }
        portfolio = fund_info['portfolio']
        date = fund_info['date']
        print(f"从用户提问提取到信息：组合: {portfolio}, 日期: {date}")
        
        # 查询资金信息
        funds_data = self._get_account_funds(portfolio, date)
        if funds_data is not None and not funds_data.empty:
            formatted_data = self._format_funds_dataframe(funds_data)
            # 提取可用资金
            matching_row = funds_data[funds_data['组合名称'] == portfolio]
            if not matching_row.empty:
                available_funds = matching_row.iloc[0]['可用资金']
            return {
                'status': 'success',
                'portfolio': portfolio,
                'date': date,
                'available_funds': available_funds,
                'data': formatted_data,
                'message': f"{portfolio}在{date}的可用资金为: {available_funds:,.2f}元"
            }
        else:
            return {
                'status': 'not_found',
                'portfolio': portfolio,
                'date': date,
                'message': f"未找到{portfolio}在{date}的资金信息"
            }

    
####################Account Position Analysis###################
class HoldingQueryProcessor:
    """独立处理持仓查询的模块"""
    
    def __init__(self, conn):
        self.conn = conn
    
    def _extract_future_info_from_query(self, user_query):
        """从用户查询中提取期货品种和日期信息"""
        import re
        
        VALID_FUTURES = {
            '螺纹钢', '热卷', '铁矿石', '焦煤', '焦炭', '动力煤', '甲醇', 'PTA', 'PP', 'PVC',
            '豆粕', '豆油', '棕榈油', '菜籽粕', '菜籽油', '玉米', '淀粉', '鸡蛋', '苹果',
            '白糖', '棉花', '橡胶', '铜', '铝', '锌', '铅', '镍', '锡', '黄金', '白银',
            '原油', '燃料油', '沥青', '天然气', '沪深300', '中证500', '上证50'
        }
        
        patterns = [
            # 品种在前的模式 - 标准格式
            r'我想知道([\u4e00-\u9fa5]+)在(\d{4}-\d{1,2}-\d{1,2})的持仓',
            r'找([\u4e00-\u9fa5]+)在(\d{4}-\d{1,2}-\d{1,2})的持仓',
            r'查询([\u4e00-\u9fa5]+)(\d{4}-\d{1,2}-\d{1,2})的持仓',
            r'([\u4e00-\u9fa5]+)品种在(\d{4}-\d{1,2}-\d{1,2})的持仓',
            r'想找([\u4e00-\u9fa5]+)在(\d{4}-\d{1,2}-\d{1,2})的持仓',
            r'([\u4e00-\u9fa5]+)在(\d{4}-\d{1,2}-\d{1,2})1的持仓',
            r'我想找([\u4e00-\u9fa5]+)品种在(\d{4}-\d{1,2}-\d{1,2})的持仓',
            r'帮我找([\u4e00-\u9fa5]+)在(\d{4}-\d{1,2}-\d{1,2})的持仓',
            r'查看([\u4e00-\u9fa5]+)在(\d{4}-\d{1,2}-\d{1,2})的持仓',
            r'获取([\u4e00-\u9fa5]+)在(\d{4}-\d{1,2}-\d{1,2})的持仓',
            
            r'我想知道([\u4e00-\u9fa5]+)(\d{4}-\d{1,2}-\d{1,2})的持仓',
            r'找([\u4e00-\u9fa5]+)(\d{4}-\d{1,2}-\d{1,2})的持仓',
            r'查询([\u4e00-\u9fa5]+)(\d{4}-\d{1,2}-\d{1,2})的持仓',
            r'([\u4e00-\u9fa5]+)(\d{4}-\d{1,2}-\d{1,2})的持仓',
            r'想找([\u4e00-\u9fa5]+)(\d{4}-\d{1,2}-\d{1,2})的持仓',
            r'([\u4e00-\u9fa5]+)品种(\d{4}-\d{1,2}-\d{1,2})的持仓',
            r'我想找([\u4e00-\u9fa5]+)(\d{4}-\d{1,2}-\d{1,2})的持仓',
            r'帮我找([\u4e00-\u9fa5]+)(\d{4}-\d{1,2}-\d{1,2})的持仓',
            r'查看([\u4e00-\u9fa5]+)(\d{4}-\d{1,2}-\d{1,2})的持仓',
            r'获取([\u4e00-\u9fa5]+)(\d{4}-\d{1,2}-\d{1,2})的持仓',
            
             # 日期在前的模式 - 标准格式
            r'(\d{4}-\d{1,2}-\d{1,2})([\u4e00-\u9fa5]+)的持仓',
            r'(\d{4}-\d{1,2}-\d{1,2})的([\u4e00-\u9fa5]+)',
            r'(\d{4}-\d{1,2}-\d{1,2})的([\u4e00-\u9fa5]+)品种的的持仓',
            r'(\d{4}-\d{1,2}-\d{1,2})的([\u4e00-\u9fa5]+)持仓',
            r'(\d{4}-\d{1,2}-\d{1,2})的([\u4e00-\u9fa5]+)持仓',
            
            # 日期在前的模式 - 年月日格式
            r'(\d{4})年(\d{1,2})月(\d{1,2})日([\u4e00-\u9fa5]+)',
            r'(\d{4})年(\d{1,2})月(\d{1,2})日的([\u4e00-\u9fa5]+)',
            r'(\d{4})年(\d{1,2})月(\d{1,2})日的([\u4e00-\u9fa5]+)品种的持仓',
            r'(\d{4})年(\d{1,2})月(\d{1,2})日的([\u4e00-\u9fa5]+)持仓',
            
            
            # 灵活匹配模式（允许中间有空格和其他字符）
            r'([\u4e00-\u9fa5]{2,5}).*?持仓.*?(\d{4}-\d{1,2}-\d{1,2})',
            r'([\u4e00-\u9fa5]{2,5}).*?持仓.*?(\d{4})年(\d{1,2})月(\d{1,2})日',
            r'持仓.*?([\u4e00-\u9fa5]{2,5}).*?(\d{4}-\d{1,2}-\d{1,2})']
        
        for pattern in patterns:
            match = re.search(pattern, user_query)
            if match:
                groups = match.groups()
                if len(groups) == 2:
                    group1, group2 = groups
                    if (re.match(r'^[\u4e00-\u9fa5]{2,5}$', group1.strip()) and 
                        re.match(r'^\d{4}-\d{1,2}-\d{1,2}$', group2.strip())):
                        variety = group1.strip()
                        date_str = group2.strip()
                    elif (re.match(r'^\d{4}-\d{1,2}-\d{1,2}$', group1.strip()) and 
                          re.match(r'^[\u4e00-\u9fa5]{2,5}$', group2.strip())):
                        variety = group2.strip()
                        date_str = group1.strip()
                        
                    if variety and date_str:
                        # 品种验证和日期标准化
                        if self._is_valid_future_variety(variety, VALID_FUTURES):
                            date_str = self._normalize_extracted_date(date_str)
                        
                        print(f"正则提取验证通过: 品种={variety}, 日期={date_str}")
                        return {
                            'variety': variety,
                            'date': date_str
                        }
        
        return self._fallback_extract_future_info(user_query)
        
    
    def _is_valid_future_variety(self, variety, valid_futures):
        """验证提取的品种是否为有效期货品种"""
        # 精确匹配
        if variety in valid_futures:
            return True
        # 包含匹配
        for valid_variety in valid_futures:
            if valid_variety in variety and len(valid_variety) >= 2:
                print(f"检测到包含关系: '{variety}' 包含有效品种 '{valid_variety}'")
                return True
        return False

    def _fallback_extract_future_info(self, user_query):
        """备选提取方法：基于关键词分词"""
        futures_varieties = [
            '螺纹钢', '热卷', '铁矿石', '焦煤', '焦炭', '动力煤', '甲醇', 'PTA', 'PP', 'PVC',
            '豆粕', '豆油', '棕榈油', '菜籽粕', '菜籽油', '玉米', '淀粉', '鸡蛋', '苹果',
            '白糖', '棉花', '橡胶', '铜', '铝', '锌', '铅', '镍', '锡', '黄金', '白银',
            '原油', '燃料油', '沥青', '天然气', '沪深300', '中证500', '上证50'
        ]
        
        found_variety = None
        for variety in futures_varieties:
            if variety in user_query:
                found_variety = variety
                break
        
        if not found_variety:
            return None
        
        date_patterns = [
            r'\d{4}-\d{1,2}-\d{1,2}',
            r'\d{4}\.\d{1,2}\.\d{1,2}',
            r'\d{4}/\d{1,2}/\d{1,2}',
            r'\d{4}年\d{1,2}月\d{1,2}日',
            r'\d{4}年\d{1,2}月\d{1,2}',
        ]
        
        found_date = None
        for pattern in date_patterns:
            match = re.search(pattern, user_query)
            if match:
                found_date = match.group()
                break
        
        if not found_date:
            return None
        
        found_date = self._normalize_extracted_date(found_date)
        if found_date:
            return {'variety': found_variety, 'date': found_date}
        
        return None
    
    def _normalize_extracted_date(self, date_str):
        """标准化提取的日期格式为 YYYY-MM-DD"""
        try:
            if re.match(r'\d{4}-\d{1,2}-\d{1,2}', date_str):
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                return date_obj.strftime('%Y-%m-%d')
            elif re.match(r'\d{4}年\d{1,2}月\d{1,2}日', date_str):
                date_str = date_str.replace('年', '-').replace('月', '-').replace('日', '')
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                return date_obj.strftime('%Y-%m-%d')
            elif re.match(r'\d{4}年\d{1,2}月\d{1,2}', date_str):
                date_str = date_str.replace('年', '-').replace('月', '')
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            return None
        return None

    def _get_holding_info(self, variety, date):
        """根据品种和日期查询持仓情况"""
        try:
            query = """
            SELECT * FROM holding_future_stock_map 
            WHERE 商品名称 LIKE :variety AND 数据日期 = TO_DATE(:date_str, 'YYYY-MM-DD')
            """
            result = pd.read_sql(query, self.conn, params={'variety': f'%{variety}%', 'date_str': date})
            
            if not result.empty:
                print(f"找到 {variety} 在 {date} 的持仓信息，共 {len(result)} 条记录")
                return result
            else:
                print(f"未找到 {variety} 在 {date} 的精确持仓信息，进行模糊匹配")
                return self._get_latest_holding_info(variety, date)
        except Exception as e:
            return None

    def _get_latest_holding_info(self, variety, date):
        """获取指定日期最新的持仓情况"""
        try:
            query = """
            SELECT * FROM holding_future_stock_map 
            WHERE 商品名称 LIKE :variety AND 数据日期 <= TO_DATE(:date_str, 'YYYY-MM-DD')
            ORDER BY 数据日期 DESC 
            LIMIT 10
            """
            result = pd.read_sql(query, self.conn, params={'variety': f'%{variety}%', 'date_str': date})
            
            if not result.empty:
                actual_date = result.iloc[0]['数据日期']
                print(f"返回 {variety} 最近日期 {actual_date} 的数据，共 {len(result)} 条记录")
                return result
            else:
                print(f"未找到 {variety} 在任何日期的持仓信息")
                return None
        except Exception as e:
            return None

    def _format_holding_dataframe(self, df):
        """格式化持仓DataFrame"""
        if df is None or df.empty:
            return df
        formatted_df = df.reset_index(drop=True)
        formatted_df.columns = [str(col) for col in formatted_df.columns]
        formatted_df = formatted_df.fillna('')
        for col in formatted_df.columns:
            if formatted_df[col].dtype == 'object':
                formatted_df[col] = formatted_df[col].astype(str).str.slice(0, 100)
        return formatted_df

    def process_holding_query(self, user_query):
        """处理持仓查询的主方法"""
        future_info = self._extract_future_info_from_query(user_query)
        if not future_info:
            return {
                'status': 'error',
                'message': "请提供具体的期货品种和日期，例如：我想知道螺纹钢在2025-07-24的持仓情况"
            }
        
        variety = future_info['variety']
        date = future_info['date']
        
        print(f"从用户提问提取到信息：品种: {variety}, 日期: {date}")
        
        holding_data = self._get_holding_info(variety, date)
        
        if holding_data is not None and not holding_data.empty:
            formatted_data = self._format_holding_dataframe(holding_data)
            return {
                'status': 'success',
                'variety': variety,
                'date': date,
                'data': formatted_data.to_dict('records'),  # 转换为字典列表供JSON返回
                'message': f"找到{variety}在{date}的持仓信息，共{len(holding_data)}条记录"
            }
        else:
            return {
                'status': 'not_found',
                'variety': variety,
                'date': date,
                'message': f"{variety}在{date}没有持仓信息"
            }


############定时任务查询模块#############
class TimedTaskQueryProcessor:
    """定时任务查询处理器"""
    
    def __init__(self, conn):
        self.conn = conn
    
    def extract_keyword_from_query(self, user_query):
        """从用户查询中提取定时任务关键词"""
        import re
        patterns = [
            r'定时任务[「"「]([^"」]+)[」"]',                    # 定时任务有引号
            r'定时任务[：:]\s*([^，。\s]+)',                   # 定时任务有冒号
            r'定时任务\s+([^，。\s]+)',                       # 定时任务有空格
            r'定时任务([^，。\s]+)(?=\s|$|的相关信息|的详情)',   # 定时任务直接匹配
        ]
        for i, pattern in enumerate(patterns, 1):
            match = re.search(pattern, user_query)
            if match:
                keyword = match.group(1).strip()
                print(f"使用模式{i}提取到定时任务关键词: '{keyword}'")
                return keyword
        print(f"所有模式都匹配失败，未从查询中提取到定时任务关键词: {user_query}")
        return None
    
    def get_timed_task_table(self):
        """获取数据库每日更新情况表格"""
        try:
            excel_path = r"\\10.101.62.31\ht-gm\实习生\数据库\数据库每日更新情况.xlsx"
            df = pd.read_excel(excel_path, sheet_name=0, header = None)  # 第一张表格
            df.columns = ['任务名称', '计划程序名称', '中文名', '定时时间', '日期更新延迟', '生成表名', 'Kettle/Python关联文件名', '关联衍生表名', '关联帆软表名', '备注']
            print(f"加载定时任务表格成功，行数: {len(df)}")
            return df
        except Exception as e:
            print(f"加载数据库每日更新情况表格失败: {e}")
            return None
    
    def filter_timed_task_table(self, table, keyword):
        """根据关键词过滤定时任务表格"""
        if table is None:
            print("定时任务表格为空，无法过滤")
            return None

        print(f"开始过滤定时任务表格，关键词：{keyword}, 表格列名：{table.columns.tolist()}")
        
        if '计划程序名称' in table.columns:
            filtered = table[table['计划程序名称'].astype(str).str.contains(keyword, na=False)]
            print(f"定时任务过滤结果行数: {len(filtered)}")
            return filtered
        else:
            print("定时任务表格中不存在'计划程序名称'列")
            return None
    
    def process_timed_task_query(self, user_query):
        """处理定时任务查询"""
        
        print("检测到定时任务查询关键词，转交定时任务模块处理")
        keyword = self.extract_keyword_from_query(user_query)
        if not keyword:
            return {
                'status': 'not_found',
                'message': '未在查询中识别到定时任务名称，请使用格式：定时任务[任务名称]',
                'query_type': 'timed_task'
            }
        # 获取表格
        table = self.get_timed_task_table()
        if table is None:
            return {
                'status': 'error',
                'message': '定时任务数据表加载失败',
                'query_type': 'timed_task'
            }
        # 过滤表格
        filtered_table = self.filter_timed_task_table(table, keyword)
        if filtered_table is not None and not filtered_table.empty:
            return {
                'status': 'success',
                'message': f"找到与'{keyword}'相关的定时任务信息",
                'data': filtered_table,
                'query_type': 'timed_task',
                'keyword': keyword
            }
        else:
            return {
                'status': 'not_found',
                'message': f"未找到与'{keyword}'相关的定时任务信息",
                'query_type': 'timed_task',
                'keyword': keyword
            }

#################帆软表查询模块###################
class FanruanTableQueryProcessor:
    """帆软表查询处理器"""
    
    def __init__(self, conn):
        self.conn = conn
    
    def extract_keyword_from_query(self, user_query):
        """从用户查询中提取帆软表关键词"""
        import re
        patterns = [
            r'帆软表[「"「]([^"」]+)[」"]',                    # 帆软表有引号
            r'帆软表[：:]\s*([^，。\s]+)',                   # 帆软表有冒号
            r'帆软表\s+([^，。\s]+)',                       # 帆软表有空格
            r'帆软表([^，。\s]+)(?=\s|$|的相关信息|的详情)',   # 帆软表直接匹配
            r'帆软表([^，。]+?)(?:\s|$|的相关信息|的详情)',    # 帆软表更灵活的匹配
        ]
        for i, pattern in enumerate(patterns, 1):
            match = re.search(pattern, user_query)
            if match:
                keyword = match.group(1).strip()
                print(f"使用模式{i}提取到帆软表关键词: '{keyword}'")
                return keyword
        print(f"所有模式都匹配失败，未从查询中提取到帆软表关键词: {user_query}")
        return None
    
    def get_fanruan_table(self):
        """获取风控结算数据说明表格"""
        try:
            excel_path = r"C:\Users\18086\Desktop\flask\华泰国贸风控结算数据说明新版.xlsx"
            df = pd.read_excel(excel_path, sheet_name=1)  # 第二张表格
            print(f"加载帆软表成功，行数: {len(df)}")
            return df
        except Exception as e:
            print(f"加载风控结算数据说明表格失败: {e}")
            return None
    
    def filter_fanruan_table(self, table, keyword):
        """根据关键词过滤帆软表"""
        if table is None:
            print("帆软表格为空，无法过滤")
            return None

        print(f"开始过滤帆软表格，关键词：{keyword}")
        print(f"表格总行数: {len(table)}")
        print(f"表格列名：{table.columns.tolist()}")
        
        if '表名' in table.columns:
            
            values = table['表名'].astype(str)
            #print(f"'表名'列的样例: {values[:10]}")  # 显示前10个，可选其他数字，这个地方用来避免缺少输出    
            contains_keyword = table[table['表名'].astype(str).str.contains(keyword, na=False)]
            print(f"初步过滤结果行数: {len(contains_keyword)}")          
            for idx, value in enumerate(table['表名'].astype(str)):
                if keyword in value:
                    print(f"  行 {idx}: '{value}' 匹配到关键词 '{keyword}'")            
            filtered = table[table['表名'].astype(str).str.contains(keyword, na=False, regex=False)]
            return filtered
        else:
            print("帆软表格中不存在'表名'列")
            return None
        
    
    def process_fanruan_query(self, user_query):
        """处理帆软表查询"""
        print("检测到帆软表查询关键词，转交帆软表模块处理")
        
        # 提取关键词
        keyword = self.extract_keyword_from_query(user_query)
        
        if not keyword:
            return {
                'status': 'not_found',
                'message': '未在查询中识别到帆软表名称，请使用格式：帆软表[表名称]',
                'query_type': 'fanruan_table'
            }
        
        # 获取表格
        table = self.get_fanruan_table()
        
        if table is None:
            return {
                'status': 'error',
                'message': '帆软表数据表加载失败',
                'query_type': 'fanruan_table'
            }
        
        # 过滤表格
        filtered_table = self.filter_fanruan_table(table, keyword)
        
        if filtered_table is not None and not filtered_table.empty:
            return {
                'status': 'success',
                'message': f"找到与'{keyword}'相关的帆软表信息",
                'data': filtered_table,
                'query_type': 'fanruan_table',
                'keyword': keyword
            }
        else:
            return {
                'status': 'not_found',
                'message': f"未找到与'{keyword}'相关的帆软表信息",
                'query_type': 'fanruan_table',
                'keyword': keyword
            }
            
            
                
####################知识库主类###################
class ExcelKnowledgeBase:
    
    def __init__(self, excel_path, code, conn):
        self.excel_path = excel_path
        self.code = code
        self.conn = conn
        
        # 避免None错误，初始化为空DataFrame
        self.df = pd.DataFrame()
        def chinese_tokenizer(text):
            return list(jieba.cut(text))
        
        # 初始化
        self.vectorizer = TfidfVectorizer(tokenizer=chinese_tokenizer)
        self.question_vectors = None
        self.profit_loss_processor = DailyProfitLossQueryProcessor(conn)
        self.holding_processor = HoldingQueryProcessor(conn)
        self.funds_processor = AccountFundsQueryProcessor(conn)
        self.timed_task_processor = TimedTaskQueryProcessor(conn)  # 定时任务模块
        self.fanruan_processor = FanruanTableQueryProcessor(conn)  # 帆软表模块
        self._load_data()
        self.answer2_processed = [] # 存储回答2处理后的数据
        self._process_answer2()  # 专门处理回答2


    def _load_data(self):
        """从Excel加载数据"""
        
        try:
            # 检查文件是否存在
            if not os.path.exists(self.excel_path):
                print(f"Excel文件不存在: {self.excel_path}")
                self.df = pd.DataFrame()  # 确保是DataFrame而不是None
                return
            
            self.df = pd.read_excel(self.excel_path)
            print(f"成功读取Excel文件，原始数据 {len(self.df)} 行")
            
            # 检查必要的列是否存在
            if '索引问题' not in self.df.columns:
                print("Excel文件中缺少'索引问题'列")
                self.df = pd.DataFrame()
                return
                
            # 处理缺失值, 删除问题为空的行
            self.df = self.df.dropna(subset=['索引问题'])
            print(f"清理后数据 {len(self.df)} 行")
            
            # 检查是否有数据
            if len(self.df) == 0:
                print("Excel文件中没有有效数据")
                self.question_vectors = None
                return
                
            # TF-IDF向量转化索引问题
            self.question_vectors = self.vectorizer.fit_transform(self.df['索引问题'].tolist())
            print(f"知识库初始化完成！加载了 {len(self.df)} 条问题")
            
        except Exception as e:
            print(f"加载Excel文件失败: {e}")
            self.df = pd.DataFrame()
            self.question_vectors = None


    def _extract_table_name(self, answer1_str):
        """从回答1文本中提取表格名称"""
        import re
        pattern = r'输出(.*?)对应信息'
        match = re.search(pattern, answer1_str)
        if match:
            table_name = match.group(1).strip()
            return table_name
        return None


    def _get_table_by_name(self, table_name):
        """根据表格名称获取对应的表格数据"""
        table_mapping = {
            '数据库每日更新情况': self._database_everyday_update_condition,
            '华泰国贸风控结算数据说明新版/帆软报表情况（新）': self._htgm_data_introduction_new
        }

        if table_name in table_mapping:
            print(f"找到匹配的表格：{table_name}")
            return table_mapping[table_name]()
        
        for key in table_mapping:
            if key in table_name or table_name in key:
                return table_mapping[key]()
        
        print(f"警告: 未找到表格: {table_name}")
        return None

    def _database_everyday_update_condition(self):
        """获取数据库每日更新情况表格"""
        try:
            excel_path = r"C:\Users\18086\Desktop\flask\数据库每日更新情况.xlsx"
            return pd.read_excel(excel_path, sheet_name=0)
        except Exception as e:
            print(f"加载数据库每日更新情况表格失败: {e}")
            return None
               
    def _htgm_data_introduction_new(self):
        """获取风控结算数据说明表格"""
        try:
            excel_path = r"C:\Users\18086\Desktop\flask\华泰国贸风控结算数据说明新版.xlsx"
            return pd.read_excel(excel_path, sheet_name=1)
        except Exception as e:
            print(f"加载风控结算数据说明表格失败: {e}")
            return None

    def _format_dataframe(self, df):
        """格式化DataFrame"""
        formatted_df = df.reset_index(drop=True)
        formatted_df.columns = [str(col) for col in formatted_df.columns]
        formatted_df = formatted_df.fillna('')
        for col in formatted_df.columns:
            if formatted_df[col].dtype == 'object':
                formatted_df[col] = formatted_df[col].astype(str).str.slice(0, 100)
        return formatted_df

    def _process_answer2(self):
        """处理所有问题的回答2"""
        self.answer2_processed = [] 
        
        if self.df is None or len(self.df) == 0:
            print("没有数据可处理，跳过answer2处理")
            return
        
        for i in range(len(self.df)):
            
            answer1_value = self.df['回答1'].iloc[i] if '回答1' in self.df.columns else ""
            answer2_value = self.df['回答2'].iloc[i] if '回答2' in self.df.columns and i < len(self.df) else ""
            
            if pd.isna(answer2_value) or str(answer2_value).strip() == '':
                self.answer2_processed.append(None)
                continue

            answer1_str = str(answer1_value).strip()
            answer2_str = str(answer2_value).strip()
            
            if any(keyword in answer2_str for keyword in ['完整dataframe加品种代码', '品种代码']):
                try:
                    product_names = [v.strip() for v in str(answer1_value).replace('、', ',').split(',') if v.strip()]
                    product_names_df = pd.DataFrame({'商品名称': product_names})
                    
                    # 检查code DataFrame是否为空
                    if self.code is not None and len(self.code) > 0:
                        filtered_code = product_names_df.merge(self.code, on='商品名称', how='left')
                        
                        if not filtered_code.empty:
                            code_dataframe = pd.DataFrame({
                                'contract_name': filtered_code['商品名称'],
                                'contract_code': filtered_code['代码'],
                            })
                            self.answer2_processed.append(code_dataframe)
                    else:
                        self.answer2_processed.append(None)
                        
                except Exception as e:
                    print(f"处理商品代码类型回答2时出错 (行 {i}): {e}")
                    self.answer2_processed.append(f"处理错误: {str(e)}")

            elif '输出' in answer1_str and '对应信息' in answer1_str:
                try:
                    table_name = self._extract_table_name(answer1_str)
                    if table_name:
                        data_table = self._get_table_by_name(table_name)
                        if data_table is not None and not data_table.empty: 
                            formatted_table = self._format_dataframe(data_table)
                            self.answer2_processed.append(formatted_table)
                            print(f"表格类型回答2处理成功，表格行数: {len(formatted_table)}")
                        else:
                            print(f"表格加载失败或为空: {table_name}")
                            self.answer2_processed.append(answer2_str)
                    else:
                        print("未提取到表格名称")
                        self.answer2_processed.append(answer2_str)
                        
                except Exception as e:
                    print(f"处理数据表查询类型回答2时出错 (行 {i}): {e}")
                    self.answer2_processed.append(f"处理错误: {str(e)}")
            else:
                self.answer2_processed.append(answer2_str)
                        
        print(f"回答2处理完成，共处理 {len(self.answer2_processed)} 个回答")
       

    def ask_question(self, user_query, similarity_threshold=0.3, top_k=1, keyword_match= False):
        """提问并获取答案"""
        # 检查问题类型 - 优先处理专用模块
        timed_task_keywords = ['定时任务', '定时表', '计划任务', '定时程序']
        if any(keyword in user_query for keyword in timed_task_keywords):
            print("检测到定时任务查询关键词，转交定时任务模块处理")
            return self.timed_task_processor.process_timed_task_query(user_query)

        fanruan_keywords = ['帆软表', '帆软报表', '帆软', '报表情况']
        if any(keyword in user_query for keyword in fanruan_keywords):
            print("检测到帆软表查询关键词，转交帆软表模块处理")
            return self.fanruan_processor.process_fanruan_query(user_query)

        holding_keywords = ['持仓情况', '持仓信息', '持仓']
        if any(keyword in user_query for keyword in holding_keywords):
            print("检测到持仓查询关键词，转交持仓模块处理")
            return self.holding_processor.process_holding_query(user_query)

        funds_keywords = ['账户资金', '可用资金', '资金情况', '资金信息', '资金余额', '账户余额', '资金查询']
        if any(keyword in user_query for keyword in funds_keywords):
            print("检测到资金查询关键词，转交资金模块处理")
            return self.funds_processor.process_funds_query(user_query)

        profit_loss_keywords = [
            '日盈亏', '总盈亏', '当日盈亏', '每日盈亏', 
            '月盈亏', '月度盈亏', '当月盈亏',
            '年盈亏', '年度盈亏', '当年盈亏',
            '盈亏情况', '盈亏信息', '盈亏查询', '盈亏多少', '盈亏如何'
        ]
        if any(keyword in user_query for keyword in profit_loss_keywords):
            print("检测到盈亏查询关键词，转交盈亏模块处理")
            return self.profit_loss_processor.process_profit_loss_query(user_query)

        # 使用原有的知识库问答逻辑
        if not hasattr(self, 'answer2_processed') or len(self.answer2_processed) != len(self.df):
            self._process_answer2()
        if self.question_vectors is None:
            return {"status": "error", "message": "知识库未初始化"}
        query_vector = self.vectorizer.transform([user_query])
        similarities = cosine_similarity(query_vector, self.question_vectors).flatten()
        results = []
        for i, score in enumerate(similarities):
            if score >= similarity_threshold:
                question_text = self.df.iloc[i]['索引问题']
                answer1_text = str(self.df.iloc[i]['回答1']) if '回答1' in self.df.columns else ""
                has_further = False
                further_answer = None
                if i < len(self.answer2_processed):
                    answer2_item = self.answer2_processed[i]              
                    if (isinstance(answer2_item, pd.DataFrame) and 
                        any(keyword in user_query for keyword in ['定时任务', '帆软表'])):         
                        if '定时任务' in user_query:
                            keyword = self.timed_task_processor.extract_keyword_from_query(user_query)
                        elif '帆软表' in user_query:
                            keyword = self.fanruan_processor.extract_keyword_from_query(user_query)
                        else:
                            keyword = None 
                        if keyword: 
                            if '数据库每日更新情况' in str(answer1_text):
                                filtered_table = self.timed_task_processor.filter_timed_task_table(answer2_item, keyword)
                            elif '帆软报表情况' in str(answer1_text):
                                filtered_table = self.fanruan_processor.filter_fanruan_table(answer2_item, keyword)
                            else:
                                filtered_table = None
                            if filtered_table is not None and not filtered_table.empty:
                                further_answer = filtered_table
                                has_further = True
                            else:
                                further_answer = f"未找到与'{keyword}'相关的信息"
                                has_further = True
                        else:
                            further_answer = answer2_item
                            has_further = True
                    elif (answer2_item is not None and 
                          ((isinstance(answer2_item, str) and str(answer2_item).strip() != '') or 
                           isinstance(answer2_item, pd.DataFrame))):
                        has_further = True
                        further_answer = answer2_item
                
                # 关键词匹配逻辑
                keyword_matched = False
                if keyword_match:
                    user_keywords = set(user_query.lower().split())
                    question_keywords = set(question_text.lower().split())
                    if user_keywords.issubset(question_keywords):
                        keyword_matched = True
                        score = min(score + 0.2, 1.0)
                result = {
                    'index': i,
                    'score': score,
                    'question': question_text,
                    'answer': answer1_text,
                    'has_further': has_further,
                    'keyword_matched': keyword_matched
                }
                if has_further:
                    result['further_answer'] = further_answer
                results.append(result)
    
        # 先按关键词匹配排序，再按相似度排序
        results.sort(key=lambda x: (-x['keyword_matched'], -x['score']))
        if not results:
            return {
                'status': 'not_found',
                'message': '未找到相关答案',
                'suggestions': ['尝试使用更具体的关键词', '检查拼写是否正确']
            }
        if top_k == 1:
            best_result = results
            return {
                'status': 'success',
                'result': best_result,
                'total_matches': len(results)
            }
        else:
            return {
                'status': 'success',
                'results': results[:top_k],
                'total_matches': len(results)
            }


pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 200)
pd.set_option('display.colheader_justify', 'left')


#########################专用dataframe生成########################
def _display_search_dataframe(df, max_rows=50):
    """简洁版的数据表格显示"""
    if df is None or df.empty:
        print("暂无数据")
        return
    
    print("="*60)
    print("数据查询结果")
    print("="*60)

    if len(df) > max_rows:
        display_df = display(df.head())
        print(f"显示前 {max_rows} 行数据（共 {len(df)} 行）")
    else:
        display_df = display(df)
    
    pd.set_option('display.width', 120)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', 20)
    
    if len(df) > max_rows:
        print(f"... 还有 {len(df) - max_rows} 行数据未显示")
    
    print(f"数据维度: {len(df)} 行 × {len(df.columns)} 列")

    return display_df


#######################定时任务，帆软表专用：输出格式转换为字典##########################
def _display_dataframe(df, max_rows=20):
        """按列输出DataFrame数据"""
        if df is None or df.empty:
            print("暂无数据")
            return

        if len(df) > max_rows:
            display_df = df.head(max_rows)
        else:
            display_df = df

        # 转换为按列的字典
        column_dict = display_df.to_dict('list')

        for col_name, values in column_dict.items():
            print(f"\n【{col_name}】")
            for i, value in enumerate(values, 1):
                print(f"{value}")    
        if len(df) > max_rows:
            print(f"\n... 还有 {len(df) - max_rows} 行未显示")



#####################全局知识库实例#############################
kb = None
@app.before_first_request
def initialize_knowledge_base():
    """在第一个请求前初始化知识库"""
    global kb
    try:
        print("=== 开始初始化知识库 ===")
        
        # 测试数据库连接
        print("测试数据库连接中...")
        conn = get_db_connection()
        if conn is None:
            print("数据库连接失败")
            kb = None
            return
        
        print("数据库连接成功")
        
        # 测试Excel文件
        print("测试Excel文件...")
        excel_path = r"C:\Users\Desktop\flask\risk_insights.xlsx"
        print(f"Excel路径: {excel_path}")
        
        if not os.path.exists(excel_path):
            print(f"Excel文件不存在: {excel_path_1}")
            kb = ExcelKnowledgeBase(excel_path, pd.DataFrame(), conn)
            return
       
        print("Excel文件存在")
        
        # 测试数据库查询
        print("测试数据库查询...")
        try:
            code = pd.read_sql('SELECT * FROM BASIC_PRODUCT', conn)[['商品名称','代码']]
            print(f"数据库查询成功，获取到 {len(code)} 条商品代码")
        except Exception as query_error:
            print(f"数据库查询失败: {query_error}")
            code = pd.DataFrame(columns=['商品名称', '代码'])
        
        # 初始化知识库
        print("初始化知识库类...")
        try:
            excel_path = r"C:\Users\Desktop\flask\risk_insights.xlsx"
            code = pd.read_sql('select * from BASIC_PRODUCT', conn)[['商品名称','代码']]  
            kb = ExcelKnowledgeBase(excel_path, code, conn)
            if len(kb.df) > 0:
                print("知识库初始化成功")
            else:
                print("知识库初始化完成，但数据为空")
        except Exception as kb_error:
            print(f"知识库类初始化失败: {kb_error}")
            import traceback
            traceback.print_exc()
            kb = None
            
    except Exception as e:
        print(f"知识库初始化总体失败: {e}")
        import traceback
        traceback.print_exc()
        kb = None
   

@app.route('/')
def index():
    """显示主页面"""
    return render_template('index.html')
    
    
@app.route('/api/ask', methods=['POST'])
def api_ask():
    """API接口：处理用户提问"""
    try:
        if kb is None:
            return jsonify({
                'status': 'error',
                'message': '系统正在初始化，请稍后重试'
            })
        
        # 检查知识库数据是否为空
        if hasattr(kb, 'df') and (kb.df is None or len(kb.df) == 0):
            return jsonify({
                'status': 'error',
                'message': '知识库数据为空，请检查Excel文件配置'
            })
        
        data = request.get_json()
        if not data or 'question' not in data:
            return jsonify({
                'status': 'error', 
                'message': '请提供问题内容'
            })
        
        user_query = data['question'].strip()
        if not user_query:
            return jsonify({
                'status': 'error',
                'message': '问题不能为空'
            })
        
        print(f"收到用户提问: {user_query}")
        
        # 调用知识库获取答案
        result = kb.ask_question(user_query)
        
        # 格式化响应
        response = {
            'status': result['status'],
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        if result['status'] == 'success':
            # 处理不同的查询类型
            if 'query_type' in result:      
                query_type = result['query_type']                     
                if query_type == 'timed_task':  # 定时任务查询
                    response.update({
                        'question_type': 'timed_task',
                        'keyword': result.get('keyword', ''),
                        'answer': result['message']
                    })
                   
                    if 'data' in result and result['data'] is not None and not result['data'].empty:
                        timed_task_data = result['data']
                        styled_data = timed_task_data.style.set_properties(**{
                            'height': '50px', 
                            'line-height': '2',
                            'white-space': 'nowrap',
                            'font-family': 'Arial, sans-serif',  # 设置字体
                            'font-size': '14px',  # 设置字体大小
                            'text-align': 'center'  # 文本居中
                        })
                        styled_data = styled_data.set_table_styles([
                            {
                                'selector': 'th.col_heading',
                                'props': [
                                    ('white-space', 'nowrap !important'),
                                    ('text-align', 'center'),
                                    ('font-family', 'Arial, sans-serif'),  # 列名字体
                                    ('font-size', '16px'),  # 列名字体大小
                                    ('font-weight', 'bold'),  # 列名加粗
                                    ('background-color', '#f8f9fa'),  # 列名背景色
                                    ('color', '#333')  # 列名文字颜色
                                ]
                            },
                            {
                                'selector': 'tbody tr',
                                'props': [
                                    ('border-bottom', '1px solid #dee2e6')  # 行间边框
                                ]
                            }
                        ])
                        response['data_html'] = styled_data.to_html(classes='table table-striped', index=False, escape=False)
                        
                elif query_type == 'fanruan_table':  # 帆软表查询
                    response.update({
                        'question_type': 'fanruan_table',
                        'keyword': result.get('keyword', ''),
                        'answer': result['message']
                    })
                  
                    if 'data' in result and result['data'] is not None and not result['data'].empty:
                        fanruan_data = result['data']
                        styled_data = fanruan_data.style.set_properties(**{
                            'height': '50px', 
                            'line-height': '2',
                            'white-space': 'nowrap',
                            'font-family': 'Arial, sans-serif',  # 设置字体
                            'font-size': '14px',  # 设置字体大小
                            'text-align': 'center'  # 文本居中
                        })
                        styled_data = styled_data.set_table_styles([
                            {
                                'selector': 'th.col_heading',
                                'props': [
                                    ('white-space', 'nowrap'),
                                    ('text-align', 'center'),
                                    ('font-family', 'Arial, sans-serif'),  # 列名字体
                                    ('font-size', '16px'),  # 列名字体大小
                                    ('font-weight', 'bold'),  # 列名加粗
                                    ('background-color', '#f8f9fa'),  # 列名背景色
                                    ('color', '#333')  # 列名文字颜色
                                ]
                            },
                            {
                                'selector': 'tbody tr',
                                'props': [
                                    ('border-bottom', '1px solid #dee2e6')  # 行间边框
                                ]
                            }
                        ])
                        response['data_html'] = styled_data.to_html(classes='table table-striped', index=False, escape=False)
            
                elif query_type:  # 盈亏查询
                    
                    total_profit_loss = result.get('total_profit_loss', 0)
                    if hasattr(total_profit_loss, 'item'):  
                        total_profit_loss = total_profit_loss.item()
                    elif hasattr(total_profit_loss, 'dtype'): 
                        total_profit_loss = float(total_profit_loss)
                    
                    response.update({
                        'question_type': 'profit_loss',
                        'date': result.get('date', ''),
                        'query_type': result['query_type'],
                        'answer': result.get('message', '盈亏查询完成'),
                        'total_profit_loss': total_profit_loss,
                        'profit_status': result.get('profit_status', 'unknown')
                    })
                        
                    if 'data' in result and result['data'] is not None:
                        try:
                           
                            if hasattr(result['data'], 'empty'):
                                if not result['data'].empty:
                                    response['data_html'] = result['data'].to_html(classes='table table-striped', index=False, escape=False)
                            
                            
                            elif isinstance(result['data'], list) and len(result['data']) > 0:
                                df = pd.DataFrame(result['data'])
                                if not df.empty:
                                    response['data_html'] = df.to_html(classes='table table-striped', index=False, escape=False)
                            
                            
                            elif isinstance(result['data'], dict) and result['data'].get('type') == 'dataframe':
                                data_list = result['data'].get('data', [])
                                if data_list:
                                    df = pd.DataFrame(data_list)
                                    response['data_html'] = df.to_html(classes='table table-striped', index=False, escape=False)
                        except Exception as e:
                            print(f"盈亏数据转换HTML失败: {e}")
                            response['data_error'] = f"数据展示失败: {str(e)}"
            
            elif 'available_funds' in result:  # 资金查询
                response.update({
                    'question_type': 'funds',
                    'portfolio': result['portfolio'],
                    'date': result['date'],
                    'answer': result['message'],
                    'available_funds': result['available_funds']
                })
                # 如果有数据表格，转换为HTML格式
                if 'data' in result and result['data'] is not None and not result['data'].empty:
                    response['data_html'] = result['data'].to_html(classes='table table-striped', index=False, escape=False)
              
            elif 'variety' in result:  # 持仓查询
                response.update({
                    'question_type': 'holding',
                    'variety': result['variety'],
                    'date': result['date'],
                    'answer': result['message']
                })
                # 如果有数据，转换为HTML格式
                if 'data' in result and result['data'] is not None:
                    if isinstance(result['data'], pd.DataFrame) and not result['data'].empty:
                        response['data_html'] = result['data'].to_html(classes='table table-striped', index=False, escape=False)
                    elif isinstance(result['data'], list) and len(result['data']) > 0:
                        df = pd.DataFrame(result['data'])
                        response['data_html'] = df.to_html(classes='table table-striped', index=False, escape=False)
            
            else:  # 普通知识库问答
                if 'results' in result:
                    response.update({
                        'question_type': 'knowledge_multiple',
                        'total_results': len(result['results'])
                    })
                    
                    results_list = []
                    for i, res in enumerate(result['results']):
                        result_item = {
                            'index': i + 1,
                            'question': res.get('question', ''),
                            'answer': res.get('answer', ''),
                            'confidence': round(res.get('score', 0), 3),
                            'has_further': res.get('has_further', False)
                        }
                        
                        # 处理进一步信息
                        if res.get('has_further'):
                            further_answer = res['further_answer']
                            if isinstance(further_answer, pd.DataFrame):
                                result_item['further_answer_html'] = further_answer.to_html(classes='table table-striped', index=False, escape=False)
                            else:
                                result_item['further_answer'] = str(further_answer)
                        
                        results_list.append(result_item)
                    
                    response['results'] = results_list
                
                # 然后检查 'result' 字段的类型
                elif 'result' in result:
                    res = result['result']
                    
                    # 检查 res 是列表还是字典
                    if isinstance(res, list) and len(res) > 0:
                        # 如果是列表，取第一个元素
                        first_result = res[0]
                        response.update({
                            'question_type': 'knowledge',
                            'question': first_result.get('question', ''),
                            'answer': first_result.get('answer', ''),
                            'confidence': round(first_result.get('score', 0), 3),
                            'has_further': first_result.get('has_further', False)
                        })
                        
                        # 处理进一步信息
                        if first_result.get('has_further'):
                            further_answer = first_result['further_answer']
                            if isinstance(further_answer, pd.DataFrame):
                                response['further_answer_html'] = further_answer.to_html(classes='table table-striped', index=False, escape=False)
                            else:
                                response['further_answer'] = str(further_answer)
                            
                    elif isinstance(res, dict):
                        # 如果是字典，直接使用
                        response.update({
                            'question_type': 'knowledge',
                            'question': res.get('question', ''),
                            'answer': res.get('answer', ''),
                            'confidence': round(res.get('score', 0), 3),
                            'has_further': res.get('has_further', False)
                        })
                        
                        # 处理进一步信息
                        if res.get('has_further'):
                            further_answer = res['further_answer']
                            if isinstance(further_answer, pd.DataFrame):
                                response['further_answer_html'] = further_answer.to_html(classes='table table-striped', index=False, escape=False)
                            else:
                                response['further_answer'] = str(further_answer)
                    else:
                        response['message'] = "无法解析的结果格式"
                    
                else:
                    response['message'] = "成功但无具体结果数据"
                
                # 如果有数据表格，转换为HTML格式
                if 'data' in result and result['data'] is not None and not result['data'].empty:
                    response['data_html'] = result['data'].to_html(classes='table table-striped', index=False, escape=False)
                    
        else:
            response['message'] = result.get('message', '未知错误')
            if 'suggestions' in result:
                response['suggestions'] = result['suggestions']

        return jsonify(response)
        
    except Exception as e:
        print(f"处理提问时出错: {e}")
        return jsonify({
            'status': 'error',
            'message': f'系统错误: {str(e)}'
        })

@app.route('/api/health')
def health_check():
    """健康检查接口"""
    return jsonify({
        'status': 'healthy',
        'knowledge_base_ready': kb is not None
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)