/*
 * Scalyr client library
 * Copyright 2011 Scalyr, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Taken from the json-simple library, by Yidong Fang and Chris Nokleberg.
 * This copy has been modified by Scalyr, Inc.; see README.txt for details.
 */

package com.scalyr.api.json;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * Parser for JSON text. Please note that JSONParser is NOT thread-safe.
 * 
 * @author FangYidong<fangyidong@yahoo.com.cn>
 */
public class JSONParser {
        public static final int S_INIT=0;
        public static final int S_IN_FINISHED_VALUE=1;//string,number,boolean,null,object,array
        public static final int S_IN_OBJECT=2;
        public static final int S_IN_ARRAY=3;
        public static final int S_PASSED_PAIR_KEY=4;
        public static final int S_IN_PAIR_VALUE=5;
        public static final int S_END=6;
        public static final int S_IN_ERROR=-1;
        
        private Yylex lexer = new Yylex((Reader)null);
        private Yytoken token = null;
        private int status = S_INIT;
        
        private int peekStatus(LinkedList<Object> statusStack){
                if(statusStack.size()==0)
                        return -1;
                return ((Integer)statusStack.getFirst()).intValue();
        }
        
    /**
     *  Reset the parser to the initial state without resetting the underlying reader.
     *
     */
    public void reset(){
        token = null;
        status = S_INIT;
    }
    
    /**
     * Reset the parser to the initial state with a new character reader.
     * 
     * @param in - The new character reader.
     * @throws IOException
     * @throws ParseException
     */
        public void reset(Reader in){
                lexer.yyreset(in);
                reset();
        }
        
        /**
         * @return The position of the beginning of the current token.
         */
        public int getPosition(){
                return lexer.getPosition();
        }
        
        public Object parse(String s) throws ParseException{
                StringReader in=new StringReader(s);
                try{
                        return parse(in);
                }
                catch(IOException ie){
                        /*
                         * Actually it will never happen.
                         */
                        throw new ParseException(-1, ParseException.ERROR_UNEXPECTED_EXCEPTION, ie);
                }
        }
        
        /**
         * Parse JSON text into java object from the input source.
         *      
         * @param in
     * @param containerFactory - Use this factory to createyour own JSON object and JSON array containers.
         * @return Instance of the following:
         *  org.json.simple.JSONObject,
         *      org.json.simple.JSONArray,
         *      java.lang.String,
         *      java.lang.Number,
         *      java.lang.Boolean,
         *      null
         * 
         * @throws IOException
         * @throws ParseException
         */
        public Object parse(Reader in) throws IOException, ParseException{
                reset(in);
                LinkedList<Object> statusStack = new LinkedList<Object>();
                LinkedList<Object> valueStack = new LinkedList<Object>();
                
                try{
                        do{
                                nextToken();
                                switch(status){
                                case S_INIT:
                                        switch(token.type){
                                        case Yytoken.TYPE_VALUE:
                                                status=S_IN_FINISHED_VALUE;
                                                statusStack.addFirst(new Integer(status));
                                                valueStack.addFirst(token.value);
                                                break;
                                        case Yytoken.TYPE_LEFT_BRACE:
                                                status=S_IN_OBJECT;
                                                statusStack.addFirst(new Integer(status));
                                                valueStack.addFirst(new JSONObject());
                                                break;
                                        case Yytoken.TYPE_LEFT_SQUARE:
                                                status=S_IN_ARRAY;
                                                statusStack.addFirst(new Integer(status));
                                                valueStack.addFirst(new JSONArray());
                                                break;
                                        default:
                                                status=S_IN_ERROR;
                                        }//inner switch
                                        break;
                                        
                                case S_IN_FINISHED_VALUE:
                                        if(token.type==Yytoken.TYPE_EOF)
                                                return valueStack.removeFirst();
                                        else
                                                throw new ParseException(getPosition(), ParseException.ERROR_UNEXPECTED_TOKEN, token);
                                        
                                case S_IN_OBJECT:
                                        switch(token.type){
                                        case Yytoken.TYPE_COMMA:
                                                break;
                                        case Yytoken.TYPE_VALUE:
                                                if(token.value instanceof String){
                                                        String key=(String)token.value;
                                                        valueStack.addFirst(key);
                                                        status=S_PASSED_PAIR_KEY;
                                                        statusStack.addFirst(new Integer(status));
                                                }
                                                else{
                                                        status=S_IN_ERROR;
                                                }
                                                break;
                                        case Yytoken.TYPE_RIGHT_BRACE:
                                                if(valueStack.size()>1){
                                                        statusStack.removeFirst();
                                                        valueStack.removeFirst();
                                                        status=peekStatus(statusStack);
                                                }
                                                else{
                                                        status=S_IN_FINISHED_VALUE;
                                                }
                                                break;
                                        default:
                                                status=S_IN_ERROR;
                                                break;
                                        }//inner switch
                                        break;
                                        
                                case S_PASSED_PAIR_KEY:
                                        switch(token.type){
                                        case Yytoken.TYPE_COLON:
                                                break;
                                        case Yytoken.TYPE_VALUE:
                                                statusStack.removeFirst();
                                                String key=(String)valueStack.removeFirst();
                                                Map<String, Object> parent=(Map<String, Object>)valueStack.getFirst();
                                                parent.put(key,token.value);
                                                status=peekStatus(statusStack);
                                                break;
                                        case Yytoken.TYPE_LEFT_SQUARE:
                                                statusStack.removeFirst();
                                                key=(String)valueStack.removeFirst();
                                                parent=(Map<String, Object>)valueStack.getFirst();
                                                List<Object> newArray=new JSONArray();
                                                parent.put(key,newArray);
                                                status=S_IN_ARRAY;
                                                statusStack.addFirst(new Integer(status));
                                                valueStack.addFirst(newArray);
                                                break;
                                        case Yytoken.TYPE_LEFT_BRACE:
                                                statusStack.removeFirst();
                                                key=(String)valueStack.removeFirst();
                                                parent=(Map<String, Object>)valueStack.getFirst();
                                                Map<String, Object> newObject=new JSONObject();
                                                parent.put(key,newObject);
                                                status=S_IN_OBJECT;
                                                statusStack.addFirst(new Integer(status));
                                                valueStack.addFirst(newObject);
                                                break;
                                        default:
                                                status=S_IN_ERROR;
                                        }
                                        break;
                                        
                                case S_IN_ARRAY:
                                        switch(token.type){
                                        case Yytoken.TYPE_COMMA:
                                                break;
                                        case Yytoken.TYPE_VALUE:
                                                List<Object> val=(List<Object>)valueStack.getFirst();
                                                val.add(token.value);
                                                break;
                                        case Yytoken.TYPE_RIGHT_SQUARE:
                                                if(valueStack.size()>1){
                                                        statusStack.removeFirst();
                                                        valueStack.removeFirst();
                                                        status=peekStatus(statusStack);
                                                }
                                                else{
                                                        status=S_IN_FINISHED_VALUE;
                                                }
                                                break;
                                        case Yytoken.TYPE_LEFT_BRACE:
                                                val=(List<Object>)valueStack.getFirst();
                                                Map<String,Object> newObject=new JSONObject();
                                                val.add(newObject);
                                                status=S_IN_OBJECT;
                                                statusStack.addFirst(new Integer(status));
                                                valueStack.addFirst(newObject);
                                                break;
                                        case Yytoken.TYPE_LEFT_SQUARE:
                                                val=(List<Object>)valueStack.getFirst();
                                                List<Object> newArray=new JSONArray();
                                                val.add(newArray);
                                                status=S_IN_ARRAY;
                                                statusStack.addFirst(new Integer(status));
                                                valueStack.addFirst(newArray);
                                                break;
                                        default:
                                                status=S_IN_ERROR;
                                        }//inner switch
                                        break;
                                case S_IN_ERROR:
                                        throw new ParseException(getPosition(), ParseException.ERROR_UNEXPECTED_TOKEN, token);
                                }//switch
                                if(status==S_IN_ERROR){
                                        throw new ParseException(getPosition(), ParseException.ERROR_UNEXPECTED_TOKEN, token);
                                }
                        }while(token.type!=Yytoken.TYPE_EOF);
                }
                catch(IOException ie){
                        throw ie;
                }
                
                throw new ParseException(getPosition(), ParseException.ERROR_UNEXPECTED_TOKEN, token);
        }
        
        private void nextToken() throws ParseException, IOException{
                token = lexer.yylex();
                if(token == null)
                        token = new Yytoken(Yytoken.TYPE_EOF, null);
        }
}