import yaml
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder
import joblib
import os
import unicodedata
import pykakasi
import jaconv
from .models import DatabaseManager
from sqlalchemy import text

import logging

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

class BankPredictor:
    """
    統合銀行予測器 - 2つのモデル: subject_code予測 + remarks予測
    """
    
    def __init__(self, config_dir: str = "config", model_dir: str = "models"):
        self.config_dir = Path(config_dir)
        self.model_dir = Path(model_dir)
        self.model_dir.mkdir(exist_ok=True)
        
        # モデル読み込み
        # 1. subject_code予測モデル（debit/credit科目コードペア）
        self.subject_code_model = self._load_subject_code_model()
        self.subject_code_encoder = self._load_subject_code_encoder()
        # 2. remarks予測モデル
        self.remarks_model = self._load_remarks_model()
        self.remarks_encoder = self._load_remarks_encoder()
        
        # データベース接続
        self.db = DatabaseManager()
        
        # ローマ字変換器
        self.kakasi = pykakasi.kakasi()
        self.kakasi.setMode('H', 'a')  # ひらがなをローマ字に
        self.kakasi.setMode('K', 'a')  # カタカナをローマ字に
        self.kakasi.setMode('J', 'a')  # 漢字をローマ字に
        self.conv = self.kakasi.getConverter()

    
    def normalize_text(self, text: str) -> str:
        """テキスト正規化：全角→半角、日本語→ローマ字"""
        if pd.isna(text) or not text:
            return ""
        
        text = str(text)
        # 全角英数字を半角に変換
        text = unicodedata.normalize('NFKC', text)
        # 全角スペースを半角スペースに変換
        text = text.replace('　', ' ')
        
        # 日本語をローマ字に変換
        try:
            text = self.conv.do(text)
        except:
            # 変換失敗時はそのまま
            pass
        
        # 英数字以外の文字を削除し、小文字に変換
        import re
        text = re.sub(r'[^a-zA-Z0-9\s]', ' ', text)
        text = re.sub(r'\s+', ' ', text)  # 複数スペースを1つに
        
        return text.lower().strip()
    
    def _load_subject_code_model(self):
        """UFJ subject_code予測モデル読み込み"""
        model_file = self.model_dir / 'ufj_subjectcode_model.pkl'
        if model_file.exists():
            try:
                model = joblib.load(model_file)
                print("subject_codeモデル読み込み完了: UFJ")
                return model
            except Exception as e:
                print(f"subject_codeモデル読み込み失敗: {e}")
        return None
    
    def _load_subject_code_encoder(self):
        """UFJ subject_codeエンコーダー読み込み"""
        encoder_file = self.model_dir / 'ufj_subjectcode_encoder.pkl'
        if encoder_file.exists():
            try:
                return joblib.load(encoder_file)
            except Exception as e:
                print(f"subject_codeエンコーダー読み込み失敗: {e}")
        return None
    
    def _load_remarks_model(self):
        """UFJ remarks機械学習モデル読み込み"""
        model_file = self.model_dir / 'ufj_remarks_model.pkl'
        if model_file.exists():
            try:
                model = joblib.load(model_file)
                print("remarksモデル読み込み完了: UFJ")
                return model
            except Exception as e:
                print(f"remarksモデル読み込み失敗: {e}")
        return None
    
    def _load_remarks_encoder(self):
        """UFJ remarksエンコーダー読み込み"""
        encoder_file = self.model_dir / 'ufj_remarks_encoder.pkl'
        if encoder_file.exists():
            try:
                return joblib.load(encoder_file)
            except Exception as e:
                print(f"remarksエンコーダー読み込み失敗: {e}")
        return None
    
    def predict_subject_code_ml(self, text: str) -> Optional[Tuple[str, str, float]]:
        """UFJ subject_code機械学習予測"""
        if not self.subject_code_model or not self.subject_code_encoder:
            return None
        
        try:
            # 特徴量作成
            prediction_prob = self.subject_code_model.predict_proba([text])[0]
            prediction = self.subject_code_model.predict([text])[0]
            
            max_prob = np.max(prediction_prob)
            
            # 予測結果をデコード
            decoded_prediction = self.subject_code_encoder.inverse_transform([prediction])[0]
            debit, credit = decoded_prediction.split('_')
            
            return (debit, credit, max_prob)
        except Exception as e:
            print(f"subject_code予測エラー: {e}")
            return None
    
    def predict_remarks_ml(self, text: str) -> Optional[Tuple[str, float]]:
        """UFJ remarks機械学習予測"""
        if not self.remarks_model or not self.remarks_encoder:
            return None
        
        try:
            # 特徴量作成
            prediction_prob = self.remarks_model.predict_proba([text])[0]
            prediction = self.remarks_model.predict([text])[0]
            
            max_prob = np.max(prediction_prob)
            
            # 予測結果をデコード
            predicted_remarks = self.remarks_encoder.inverse_transform([prediction])[0]
            
            return (predicted_remarks, max_prob)
        except Exception as e:
            print(f"remarks予測エラー: {e}")
            return None
    
    def get_training_data(self, target: str = 'subject_code') -> pd.DataFrame:
        """UFJ学習データをtrainディレクトリから取得
        Args:
            target: 'subject_code' または 'remarks' を指定
        """
        train_dir = Path("data/train")
        training_data = []
        
        # trainディレクトリのCSVファイルを読み込み
        for csv_file in train_dir.glob("ufj_processed_*.csv"):
            try:
                df = pd.read_csv(csv_file, encoding='utf-8')
                
                # テキストカラムを結合（combined_textがない場合）
                if 'combined_text' not in df.columns:
                    text_columns = []
                    for col in ['abstruct', 'memo']:
                        if col in df.columns:
                            text_columns.append(col)
                    
                    if text_columns:
                        df['combined_text'] = ''
                        for col in text_columns:
                            df['combined_text'] += df[col].fillna('').astype(str) + ' '
                
                # 正規化
                if 'combined_text' in df.columns:
                    df['normalized_text'] = df['combined_text'].apply(self.normalize_text)
                    
                    if target == 'subject_code':
                        # subject_code学習用データ
                        if 'suggested_debit' in df.columns and 'suggested_credit' in df.columns:
                            df['label'] = df['suggested_debit'].astype(str) + '_' + df['suggested_credit'].astype(str)
                            
                            valid_data = df[
                                (df['normalized_text'].str.len() > 0) & 
                                (df['label'].notna())
                            ][['normalized_text', 'label']]
                            
                            training_data.append(valid_data)
                    
                    elif target == 'remarks':
                        # remarks学習用データ
                        if 'remarks_classified' in df.columns:
                            valid_data = df[
                                (df['normalized_text'].str.len() > 0) & 
                                (df['remarks_classified'].notna()) &
                                (df['remarks_classified'].str.len() > 0)
                            ][['normalized_text', 'remarks_classified']]
                            
                            if not valid_data.empty:
                                valid_data.columns = ['text', 'label']
                                training_data.append(valid_data)
                        
            except Exception as e:
                print(f"学習データ読み込みエラー ({csv_file}): {e}")
                continue
        
        if training_data:
            result = pd.concat(training_data, ignore_index=True)
            if target == 'subject_code':
                result.columns = ['text', 'label']
            return result
        else:
            return pd.DataFrame(columns=['text', 'label'])
    
    def train_model(self) -> bool:
        """UFJ subject_codeモデルとremarksモデル学習"""
        success_count = 0
        
        # 1. subject_codeモデル学習
        subject_code_data = self.get_training_data('subject_code')
        if not subject_code_data.empty and len(subject_code_data) >= 10:
            try:
                # パイプライン作成
                pipeline = Pipeline([
                    ('tfidf', TfidfVectorizer(max_features=1000, ngram_range=(1, 2))),
                    ('nb', MultinomialNB())
                ])
                
                # ラベルエンコーダー
                subject_code_encoder = LabelEncoder()
                encoded_labels = subject_code_encoder.fit_transform(subject_code_data['label'])
                
                # 学習実行
                pipeline.fit(subject_code_data['text'], encoded_labels)
                
                # モデル保存
                model_file = self.model_dir / 'ufj_subjectcode_model.pkl'
                encoder_file = self.model_dir / 'ufj_subjectcode_encoder.pkl'
                
                joblib.dump(pipeline, model_file)
                joblib.dump(subject_code_encoder, encoder_file)
                
                # メモリ更新
                self.subject_code_model = pipeline
                self.subject_code_encoder = subject_code_encoder
                
                print(f"subject_codeモデル学習完了: UFJ ({len(subject_code_data)}件)")
                success_count += 1
                
            except Exception as e:
                print(f"subject_codeモデル学習失敗: {e}")
        else:
            print(f"subject_code学習データ不足: UFJ ({len(subject_code_data)}件)")
        
        # 2. remarksモデル学習
        remarks_data = self.get_training_data('remarks')
        if not remarks_data.empty and len(remarks_data) >= 10:
            try:
                # パイプライン作成
                remarks_pipeline = Pipeline([
                    ('tfidf', TfidfVectorizer(max_features=1000, ngram_range=(1, 2))),
                    ('nb', MultinomialNB())
                ])
                
                # ラベルエンコーダー
                remarks_label_encoder = LabelEncoder()
                encoded_remarks_labels = remarks_label_encoder.fit_transform(remarks_data['label'])
                
                # 学習実行
                remarks_pipeline.fit(remarks_data['text'], encoded_remarks_labels)
                
                # モデル保存
                remarks_model_file = self.model_dir / 'ufj_remarks_model.pkl'
                remarks_encoder_file = self.model_dir / 'ufj_remarks_encoder.pkl'
                
                joblib.dump(remarks_pipeline, remarks_model_file)
                joblib.dump(remarks_label_encoder, remarks_encoder_file)
                
                # メモリ更新
                self.remarks_model = remarks_pipeline
                self.remarks_encoder = remarks_label_encoder
                
                print(f"remarksモデル学習完了: UFJ ({len(remarks_data)}件)")
                success_count += 1
                
            except Exception as e:
                print(f"remarksモデル学習失敗: {e}")
        else:
            print(f"remarks学習データ不足: UFJ ({len(remarks_data)}件)")
        
        return success_count > 0
    
    def save_training_data(self, df: pd.DataFrame, filename: str = None) -> str:
        """学習データ保存（dateと予測で使った列のみ）"""
        train_dir = Path("data/train")
        train_dir.mkdir(exist_ok=True)

        if filename is None:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"ufj_processed_{timestamp}.csv"
        
        # dateと予測で使った列のみ（combined_textは正規化済みローマ字）
        prediction_input_columns = ['date', 'abstruct', 'memo', 'combined_text']
        prediction_output_columns = ['suggested_debit', 'suggested_credit', 'remarks_classified']
        required_columns = prediction_input_columns + prediction_output_columns
        
        # 存在するカラムのみ選択
        available_columns = [col for col in required_columns if col in df.columns]
        df_filtered = df[available_columns]
        
        output_path = train_dir / filename
        df_filtered.to_csv(output_path, index=False, encoding='utf-8')
        
        print(f"学習データ保存: {output_path} ({len(df_filtered)}行, 保存列: {available_columns})")
        return str(output_path)